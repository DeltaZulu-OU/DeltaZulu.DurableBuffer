using System.Threading.Channels;
using DeltaZulu.DurableBuffer.Abstractions;
using DeltaZulu.DurableBuffer.Chunks;
using DeltaZulu.DurableBuffer.Configuration;
using DeltaZulu.DurableBuffer.Metrics;
using DeltaZulu.DurableBuffer.Recovery;
using DeltaZulu.DurableBuffer.Rx;
using DeltaZulu.DurableBuffer.Storage;
using Microsoft.Extensions.Logging;

namespace DeltaZulu.DurableBuffer;

public sealed class DurableBufferHost<T> : IAsyncDisposable
{
    private readonly DurableBuffer<T> _buffer;
    private readonly ChunkCatalog _catalog = new();
    private readonly Channel<StoredChunk> _chunkChannel;
    private readonly SemaphoreSlim _dispatchSignal = new(0, int.MaxValue);
    private readonly BufferEventBroadcaster _eventBroadcaster;
    private readonly ILogger? _logger;
    private readonly BufferMetricsCounter _metrics;
    private readonly DurableBufferOptions _options;
    private readonly DurableBufferReader _reader;
    private readonly FileSystemRecoveryManager _recoveryManager;
    private readonly RxChunkPublisher _rxChunkPublisher;
    private readonly FileChunkStore _store;
    private CancellationTokenSource? _cts;
    private Task? _dispatcherTask;
    private bool _disposed;
    private Task? _rotationTask;
    private bool _started;

    public DurableBufferHost(
        DurableBufferOptions options,
        IRecordSerializer<T> serializer,
        ILoggerFactory? loggerFactory = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.DispatchChannelCapacity, 0);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.MaxInFlightChunks, 0);

        _options = options;
        _logger = loggerFactory?.CreateLogger<DurableBufferHost<T>>();

        _eventBroadcaster = new BufferEventBroadcaster();
        _metrics = new BufferMetricsCounter();
        _metrics.UpdateDiskUsage(0, options.MaxDiskBytes);
        _metrics.UpdateDispatchQueueCapacity(options.DispatchChannelCapacity);
        _metrics.UpdateMaxInFlightChunks(options.MaxInFlightChunks);
        _metrics.UpdateDispatcherWaitReason(DispatchWaitReason.None);

        _chunkChannel = Channel.CreateBounded<StoredChunk>(
            new BoundedChannelOptions(options.DispatchChannelCapacity) {
                SingleReader = false,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait,
                AllowSynchronousContinuations = false
            });

        _store = new FileChunkStore(
            options.StoragePath,
            loggerFactory?.CreateLogger<FileChunkStore>(),
            options.MaxDeadLetterBytes,
            options.MaxQuarantineBytes,
            onDeadLetterEvicted: evicted => {
                _metrics.AddDeadLetterBytes(-evicted.Metadata.PayloadBytes);
                _metrics.ChunkDeadLetterEvicted();
                _eventBroadcaster.Publish(BufferEvent.Create(
                    BufferEventType.BufferDeadLetterEvicted, evicted.Id.Value,
                    detail: "Dead-letter capacity exceeded; evicted oldest chunk", chunk: evicted));
            },
            onQuarantineEvicted: (path, bytes) => {
                _metrics.AddQuarantineBytes(-bytes);
                _metrics.ChunkQuarantineEvicted();
                _eventBroadcaster.Publish(BufferEvent.Create(
                    BufferEventType.BufferQuarantineEvicted,
                    detail: $"Quarantine capacity exceeded; evicted {Path.GetFileName(path)}"));
            });

        var backpressure = new BackpressureController(options);

        _buffer = new DurableBuffer<T>(
            serializer,
            _store,
            options,
            _metrics,
            _eventBroadcaster,
            backpressure,
            OnChunkSealedAsync,
            TryDropOldestAvailableChunkAsync);

        _reader = new DurableBufferReader(
            _chunkChannel.Reader,
            _store,
            _metrics,
            _eventBroadcaster,
            _buffer.SignalSpaceAvailable,
            OnChunkReleasedAsync,
            OnChunkTerminal);
        _rxChunkPublisher = new RxChunkPublisher(_chunkChannel.Reader, _eventBroadcaster);

        _recoveryManager = new FileSystemRecoveryManager(
            _store,
            _metrics,
            _eventBroadcaster,
            options.StoragePath,
            loggerFactory?.CreateLogger<FileSystemRecoveryManager>());
    }

    public IObservable<BufferEvent> Events => _eventBroadcaster;
    public IDurableBufferReader Reader => _reader;
    public IRxDispatchDiagnostics<StoredChunk> RxChunkDiagnostics => _rxChunkPublisher;
    public IRxPublisher<StoredChunk> RxChunks => _rxChunkPublisher;
    public IRxEventStream<BufferEvent> RxEvents => _eventBroadcaster;
    public IDurableBufferWriter<T> Writer => _buffer;

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        if (_started)
        {
            await StopAsync();
        }

        _eventBroadcaster.Complete();
        _rxChunkPublisher.Dispose();
        _buffer.Dispose();
        _cts?.Dispose();
        _dispatchSignal.Dispose();
        _disposed = true;
    }

    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_started)
        {
            throw new InvalidOperationException("Buffer host is already started.");
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        _eventBroadcaster.Publish(BufferEvent.Create(BufferEventType.BufferStarted));

        var recovery = await _recoveryManager.RecoverChunksAsync(_cts.Token);
        foreach (var chunk in recovery.RecoveredChunks)
        {
            _catalog.AddAvailable(chunk);
        }

        UpdateDispatchPressure(DispatchWaitReason.None);

        var diskUsed = await _store.GetDiskBytesUsedAsync(_cts.Token);
        _metrics.UpdateDiskUsage(diskUsed, _options.MaxDiskBytes);

        var deadLetterBytesUsed = await _store.GetDeadLetterBytesUsedAsync(_cts.Token);
        _metrics.UpdateDeadLetterUsage(deadLetterBytesUsed, _options.MaxDeadLetterBytes);

        var quarantineBytesUsed = await _store.GetQuarantineBytesUsedAsync(_cts.Token);
        _metrics.UpdateQuarantineUsage(quarantineBytesUsed, _options.MaxQuarantineBytes);

        _dispatcherTask = Task.Run(() => RunDispatcherAsync(_cts.Token), _cts.Token);
        _rotationTask = Task.Run(() => RunRotationTimerAsync(_cts.Token), _cts.Token);

        SignalDispatcher();

        _started = true;
        _logger?.LogInformation("Buffer host started. Storage: {Path}. Recovered={Recovered}", _options.StoragePath, recovery.Summary.RecoveredChunks);
    }

    public async ValueTask StopAsync(CancellationToken cancellationToken = default)
    {
        if (!_started || _disposed)
        {
            return;
        }

        _buffer.MarkStopping();
        _logger?.LogInformation("Buffer host stopping...");

        await _buffer.FlushAsync(cancellationToken);

        if (_cts is not null)
        {
            await _cts.CancelAsync();
        }

        SignalDispatcher();

        if (_dispatcherTask is not null)
        {
            try { await _dispatcherTask.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken); }
            catch (TimeoutException) { }
            catch (OperationCanceledException) { }
        }

        DispatchAvailableChunksForStop();
        _chunkChannel.Writer.TryComplete();

        if (_rotationTask is not null)
        {
            try { await _rotationTask.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken); }
            catch (TimeoutException) { }
            catch (OperationCanceledException) { }
        }

        UpdateDispatchPressure(DispatchWaitReason.Stopping);
        _eventBroadcaster.Publish(BufferEvent.Create(BufferEventType.BufferStopped));

        _started = false;
        _logger?.LogInformation("Buffer host stopped.");
    }

    private void DispatchAvailableChunksForStop()
    {
        while (_catalog.Snapshot().EnqueuedCount < _options.MaxInFlightChunks &&
               _catalog.TryDequeueAvailable(out var chunk))
        {
            if (!_chunkChannel.Writer.TryWrite(chunk))
            {
                _catalog.TryMarkAvailable(chunk);
                UpdateDispatchPressure(DispatchWaitReason.DispatchQueueFull);
                return;
            }
        }

        UpdateDispatchPressure(DispatchWaitReason.None);
    }

    private ValueTask OnChunkReleasedAsync(StoredChunk chunk, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _catalog.TryMarkAvailable(chunk);
        UpdateDispatchPressure(DispatchWaitReason.None);
        SignalDispatcher();
        return ValueTask.CompletedTask;
    }

    private ValueTask OnChunkSealedAsync(StoredChunk chunk, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _catalog.AddAvailable(chunk);
        UpdateDispatchPressure(DispatchWaitReason.None);
        SignalDispatcher();
        return ValueTask.CompletedTask;
    }

    private void OnChunkTerminal(StoredChunk chunk)
    {
        _catalog.TryMarkTerminal(chunk);
        UpdateDispatchPressure(DispatchWaitReason.None);
        SignalDispatcher();
    }

    private async Task RunDispatcherAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (!await _chunkChannel.Writer.WaitToWriteAsync(cancellationToken))
                {
                    return;
                }

                var snapshot = _catalog.Snapshot();
                if (snapshot.EnqueuedCount >= _options.MaxInFlightChunks)
                {
                    UpdateDispatchPressure(DispatchWaitReason.InFlightLimitReached);
                    await _dispatchSignal.WaitAsync(cancellationToken);
                    continue;
                }

                if (!_catalog.TryDequeueAvailable(out var chunk))
                {
                    UpdateDispatchPressure(DispatchWaitReason.NoAvailableChunks);
                    await _dispatchSignal.WaitAsync(cancellationToken);
                    continue;
                }

                UpdateDispatchPressure(DispatchWaitReason.None);
                await _chunkChannel.Writer.WriteAsync(chunk, cancellationToken);
                UpdateDispatchPressure(DispatchWaitReason.None);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (ChannelClosedException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error in dispatcher loop");
                UpdateDispatchPressure(DispatchWaitReason.DispatchQueueFull);
                await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);
            }
        }
    }

    private async Task RunRotationTimerAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(500));
        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            try
            {
                await _buffer.RotateIfStaleAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error in rotation timer");
            }
        }
    }

    private void SignalDispatcher()
    {
        try { _dispatchSignal.Release(); }
        catch (SemaphoreFullException) { }
    }

    private async ValueTask<bool> TryDropOldestAvailableChunkAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!_catalog.TryRemoveOldestAvailable(out var oldest))
        {
            return false;
        }

        await _store.DeleteAsync(oldest, cancellationToken);
        _metrics.AddDiskBytes(-oldest.Metadata.PayloadBytes);
        _eventBroadcaster.Publish(BufferEvent.Create(
            BufferEventType.BufferChunkDropped,
            oldest.Id.Value,
            detail: "Dropped oldest available chunk to free space",
            chunk: oldest));

        UpdateDispatchPressure(DispatchWaitReason.None);
        return true;
    }

    private void UpdateDispatchPressure(DispatchWaitReason waitReason)
    {
        var catalogSnapshot = _catalog.Snapshot();
        _metrics.UpdateDispatchQueueDepth(_chunkChannel.Reader.CanCount ? _chunkChannel.Reader.Count : 0);
        _metrics.UpdateInFlightChunks(catalogSnapshot.EnqueuedCount);
        _metrics.UpdateAvailableChunks(catalogSnapshot.AvailableCount);
        _metrics.UpdateOldestAvailableChunkAge(catalogSnapshot.OldestAvailableAge);
        _metrics.UpdateOldestDispatchedChunkAge(catalogSnapshot.OldestEnqueuedAge);
        _metrics.UpdateDispatcherWaitReason(waitReason);
    }
}
