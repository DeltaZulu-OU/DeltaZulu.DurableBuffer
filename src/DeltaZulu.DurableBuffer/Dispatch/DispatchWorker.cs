using System.Threading.Channels;
using DeltaZulu.DurableBuffer.Chunks;
using DeltaZulu.DurableBuffer.Configuration;
using DeltaZulu.DurableBuffer.Metrics;
using DeltaZulu.DurableBuffer.Retry;
using DeltaZulu.DurableBuffer.Storage;
using Microsoft.Extensions.Logging;

namespace DeltaZulu.DurableBuffer.Dispatch;

internal sealed class DispatchWorker
{
    private readonly ChannelReader<StoredChunk> _reader;
    private readonly IChunkSender _sender;
    private readonly IChunkStore _store;
    private readonly IRetryScheduler _retryScheduler;
    private readonly DurableBufferOptions _options;
    private readonly BufferMetricsCounter _metrics;
    private readonly BufferEventBroadcaster _events;
    private readonly ILogger? _logger;
    private readonly Action? _signalSpaceAvailable;
    private readonly PriorityQueue<StoredChunk, DateTimeOffset> _retryQueue = new();
    private int _retryQueueDepth;

    public DispatchWorker(
        ChannelReader<StoredChunk> reader,
        IChunkSender sender,
        IChunkStore store,
        IRetryScheduler retryScheduler,
        DurableBufferOptions options,
        BufferMetricsCounter metrics,
        BufferEventBroadcaster events,
        ILogger? logger = null,
        Action? signalSpaceAvailable = null)
    {
        _reader = reader;
        _sender = sender;
        _store = store;
        _retryScheduler = retryScheduler;
        _options = options;
        _metrics = metrics;
        _events = events;
        _logger = logger;
        _signalSpaceAvailable = signalSpaceAvailable;
    }

    public int RetryQueueDepth => Volatile.Read(ref _retryQueueDepth);

    public async Task DrainStoredChunksAsync(CancellationToken cancellationToken)
    {
        await DrainRemainingAsync(cancellationToken);
        await DrainScheduledRetriesAsync(cancellationToken);

        while (!cancellationToken.IsCancellationRequested)
        {
            var sealedChunks = await _store.GetSealedChunksAsync(cancellationToken);
            if (sealedChunks.Count == 0)
            {
                break;
            }

            foreach (var chunk in sealedChunks.OrderBy(static chunk =>
                         chunk.Metadata.NextAttemptUtc ?? chunk.Metadata.SealedUtc ?? chunk.Metadata.CreatedUtc))
            {
                var nextAttemptUtc = chunk.Metadata.NextAttemptUtc;
                if (nextAttemptUtc is not null)
                {
                    var waitTime = nextAttemptUtc.Value - DateTimeOffset.UtcNow;
                    if (waitTime > TimeSpan.Zero)
                    {
                        await Task.Delay(waitTime, cancellationToken);
                    }
                }

                await ProcessChunkAsync(chunk, cancellationToken);
                await DrainScheduledRetriesAsync(cancellationToken);
            }
        }
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            await ProcessDueRetriesAsync(cancellationToken);

            if (_reader.TryRead(out var chunk))
            {
                await ProcessChunkAsync(chunk, cancellationToken);
                continue;
            }

            var channelCompleted = _reader.Completion.IsCompleted;
            var waitTime = GetNextRetryWait();
            if (channelCompleted)
            {
                if (waitTime == TimeSpan.MaxValue)
                {
                    break;
                }

                if (waitTime > TimeSpan.Zero)
                {
                    await Task.Delay(waitTime, cancellationToken);
                }

                continue;
            }

            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                if (waitTime < TimeSpan.MaxValue)
                {
                    cts.CancelAfter(waitTime);
                }

                var hasMoreChunks = await _reader.WaitToReadAsync(cts.Token);
                if (!hasMoreChunks)
                {
                    if (waitTime == TimeSpan.MaxValue)
                    {
                        break;
                    }

                    await Task.Delay(waitTime, cancellationToken);
                }
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
            }
        }

        if (!cancellationToken.IsCancellationRequested)
        {
            await DrainStoredChunksAsync(cancellationToken);
        }
    }

    private async Task ProcessDueRetriesAsync(CancellationToken cancellationToken)
    {
        var due = new List<StoredChunk>();
        while (_retryQueue.TryPeek(out _, out var next) && next <= DateTimeOffset.UtcNow)
        {
            due.Add(_retryQueue.Dequeue());
        }

        Volatile.Write(ref _retryQueueDepth, _retryQueue.Count);
        _metrics.UpdateRetryQueueDepth(_retryQueue.Count);

        foreach (var chunk in due)
        {
            await ProcessChunkAsync(chunk, cancellationToken);
        }
    }

    private async Task ProcessChunkAsync(StoredChunk chunk, CancellationToken cancellationToken)
    {
        StoredChunk dispatching;
        try
        {
            dispatching = await _store.MoveToDispatchingAsync(chunk, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to move chunk {ChunkId} to dispatching", chunk.Id);
            return;
        }

        _metrics.ChunkSent();
        _events.Publish(BufferEvent.Create(
            BufferEventType.BufferChunkDispatchStarted, dispatching.Id.Value, chunk: dispatching));

        ChunkSendResult result;
        try
        {
            result = await _sender.SendAsync(dispatching, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await _store.MoveToSealedAsync(dispatching, dispatching.Metadata, cancellationToken);
            throw;
        }
        catch (Exception ex)
        {
            result = new ChunkSendResult(ChunkSendStatus.TransientFailure, ex.Message);
        }

        switch (result.Status)
        {
            case ChunkSendStatus.Success:
                await _store.DeleteAsync(dispatching, cancellationToken);
                _metrics.ChunkDelivered();
                _metrics.AddDiskBytes(-dispatching.Metadata.PayloadBytes);
                _signalSpaceAvailable?.Invoke();
                _events.Publish(BufferEvent.Create(
                    BufferEventType.BufferChunkDispatchSucceeded, dispatching.Id.Value, chunk: dispatching));
                break;

            case ChunkSendStatus.TransientFailure:
                await HandleTransientFailureAsync(dispatching, result.Error, cancellationToken);
                break;

            case ChunkSendStatus.PermanentFailure:
                var deadLettered = await _store.MoveToDeadLetterAsync(dispatching, cancellationToken);
                _metrics.ChunkDeadLettered();
                _events.Publish(BufferEvent.Create(
                    BufferEventType.BufferChunkDeadLettered, deadLettered.Id.Value,
                    detail: result.Error, chunk: deadLettered));
                break;
        }
    }

    private async Task HandleTransientFailureAsync(
        StoredChunk chunk,
        string? error,
        CancellationToken cancellationToken)
    {
        var newAttemptCount = chunk.Metadata.AttemptCount + 1;

        _metrics.ChunkFailed();
        _events.Publish(BufferEvent.Create(
            BufferEventType.BufferChunkDispatchFailed, chunk.Id.Value, detail: error, chunk: chunk));

        if (_retryScheduler.IsRetryExhausted(newAttemptCount))
        {
            if (_options.RetryExhaustedPolicy == RetryExhaustedPolicy.DeadLetter)
            {
                var deadLettered = await _store.MoveToDeadLetterAsync(chunk, cancellationToken);
                _metrics.ChunkDeadLettered();
                _events.Publish(BufferEvent.Create(
                    BufferEventType.BufferChunkDeadLettered, deadLettered.Id.Value,
                    detail: "Retry exhausted", chunk: deadLettered));
            }
            else
            {
                await _store.DeleteAsync(chunk, cancellationToken);
                _metrics.AddDiskBytes(-chunk.Metadata.PayloadBytes);
                _signalSpaceAvailable?.Invoke();
                _events.Publish(BufferEvent.Create(
                    BufferEventType.BufferChunkDropped, chunk.Id.Value,
                    detail: "Retry exhausted, discarded", chunk: chunk));
            }
            return;
        }

        var nextAttempt = _retryScheduler.CalculateNextAttempt(newAttemptCount);
        var updatedMetadata = chunk.Metadata with {
            AttemptCount = newAttemptCount,
            NextAttemptUtc = nextAttempt,
            LastError = error
        };

        var sealed_ = await _store.MoveToSealedAsync(chunk, updatedMetadata, cancellationToken);

        _retryQueue.Enqueue(sealed_, nextAttempt);
        Volatile.Write(ref _retryQueueDepth, _retryQueue.Count);
        _metrics.ChunkRetried();
        _metrics.UpdateRetryQueueDepth(_retryQueue.Count);

        _events.Publish(BufferEvent.Create(
            BufferEventType.BufferChunkRetryScheduled, sealed_.Id.Value,
            detail: $"Attempt {newAttemptCount}, next at {nextAttempt:O}", chunk: sealed_));
    }

    private async Task DrainRemainingAsync(CancellationToken cancellationToken)
    {
        while (_reader.TryRead(out var chunk))
        {
            try
            {
                await ProcessChunkAsync(chunk, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task DrainScheduledRetriesAsync(CancellationToken cancellationToken)
    {
        while (_retryQueue.Count > 0 && !cancellationToken.IsCancellationRequested)
        {
            var waitTime = GetNextRetryWait();
            if (waitTime > TimeSpan.Zero)
            {
                await Task.Delay(waitTime, cancellationToken);
            }

            await ProcessDueRetriesAsync(cancellationToken);
        }
    }

    private TimeSpan GetNextRetryWait()
    {
        if (_retryQueue.TryPeek(out _, out var next))
        {
            var delay = next - DateTimeOffset.UtcNow;
            return delay > TimeSpan.Zero ? delay : TimeSpan.Zero;
        }
        return TimeSpan.MaxValue;
    }
}