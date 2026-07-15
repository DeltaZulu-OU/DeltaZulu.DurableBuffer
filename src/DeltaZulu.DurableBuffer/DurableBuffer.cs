using DeltaZulu.DurableBuffer.Abstractions;
using DeltaZulu.DurableBuffer.Chunks;
using DeltaZulu.DurableBuffer.Configuration;
using DeltaZulu.DurableBuffer.Metrics;
using DeltaZulu.DurableBuffer.Storage;

namespace DeltaZulu.DurableBuffer;

internal sealed class DurableBuffer<T> : IDurableBufferWriter<T>, IDisposable
{
    private readonly IRecordSerializer<T> _serializer;
    private readonly IChunkStore _store;
    private readonly DurableBufferOptions _options;
    private readonly BufferMetricsCounter _metrics;
    private readonly BufferEventBroadcaster _events;
    private readonly BackpressureController _backpressure;
    private readonly Func<StoredChunk, CancellationToken, ValueTask> _onChunkSealed;
    private readonly Func<CancellationToken, ValueTask<bool>> _tryDropOldestChunk;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly ChunkBuilder _chunkBuilder;
    private volatile bool _stopping;
    private readonly SemaphoreSlim _spaceAvailable = new(0, int.MaxValue);

    public DurableBuffer(
        IRecordSerializer<T> serializer,
        IChunkStore store,
        DurableBufferOptions options,
        BufferMetricsCounter metrics,
        BufferEventBroadcaster events,
        BackpressureController backpressure,
        Func<StoredChunk, CancellationToken, ValueTask> onChunkSealed,
        Func<CancellationToken, ValueTask<bool>> tryDropOldestChunk)
    {
        _serializer = serializer;
        _store = store;
        _options = options;
        _metrics = metrics;
        _events = events;
        _backpressure = backpressure;
        _onChunkSealed = onChunkSealed;
        _tryDropOldestChunk = tryDropOldestChunk;
        _chunkBuilder = new ChunkBuilder(options);

        _metrics.UpdateDiskUsage(0, options.MaxDiskBytes);
        _metrics.ChunkCreated();
        _events.Publish(BufferEvent.Create(BufferEventType.BufferChunkOpened, _chunkBuilder.ChunkId.Value));
    }

    internal void MarkStopping() => _stopping = true;

    internal void SignalSpaceAvailable()
    {
        try { _spaceAvailable.Release(); }
        catch (SemaphoreFullException) { }
    }

    public async ValueTask<BufferWriteResult> WriteAsync(
        T record,
        CancellationToken cancellationToken = default)
    {
        if (_stopping)
        {
            _metrics.RecordRejected();
            return new BufferWriteResult(BufferWriteStatus.RejectedStopping);
        }

        ReadOnlyMemory<byte> serialized;
        try
        {
            serialized = _serializer.Serialize(record);
        }
        catch
        {
            _metrics.RecordRejected();
            return new BufferWriteResult(BufferWriteStatus.RejectedRecordTooLarge);
        }

        if (serialized.Length + ChunkFormat.RecordLengthSize + ChunkFormat.HeaderSize + ChunkFormat.FooterSize >
            _options.MaxChunkBytes)
        {
            _metrics.RecordRejected();
            _events.Publish(BufferEvent.Create(BufferEventType.BufferRecordRejected, detail: "Record too large"));
            return new BufferWriteResult(BufferWriteStatus.RejectedRecordTooLarge);
        }

        var writeStatus = BufferWriteStatus.Accepted;
        var pressureResult = await HandleBackpressureAsync(cancellationToken);
        if (pressureResult.HasValue)
        {
            if (pressureResult.Value.Status != BufferWriteStatus.DroppedOldestAndAccepted)
            {
                return pressureResult.Value;
            }

            writeStatus = BufferWriteStatus.DroppedOldestAndAccepted;
        }

        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            if (_chunkBuilder.WouldExceedLimit(serialized.Length) && !_chunkBuilder.IsEmpty)
            {
                await RotateChunkAsync(cancellationToken);
            }

            _chunkBuilder.Append(serialized);
            _metrics.RecordAccepted();
            _metrics.UpdateOpenChunkBytes(_chunkBuilder.CurrentBytes);
            _metrics.UpdateMemoryUsage(_chunkBuilder.CurrentBytes);

            if (_chunkBuilder.ShouldRotate)
            {
                await RotateChunkAsync(cancellationToken);
            }

            return new BufferWriteResult(writeStatus);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            if (!_chunkBuilder.IsEmpty)
            {
                await RotateChunkAsync(cancellationToken);
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public BufferSnapshot GetSnapshot() => _metrics.ToSnapshot();

    internal async ValueTask RotateIfStaleAsync(CancellationToken cancellationToken)
    {
        if (!await _writeLock.WaitAsync(0, cancellationToken))
        {
            return;
        }

        try
        {
            if (_chunkBuilder.ShouldRotate && !_chunkBuilder.IsEmpty)
            {
                await RotateChunkAsync(cancellationToken);
            }
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async ValueTask RotateChunkAsync(CancellationToken cancellationToken)
    {
        var (data, metadata) = _chunkBuilder.Seal();

        var storedChunk = await _store.SealAsync(
            _chunkBuilder.ChunkId, data, metadata, cancellationToken);

        _metrics.ChunkSealed();
        _metrics.AddDiskBytes(data.Length);
        _events.Publish(BufferEvent.Create(
            BufferEventType.BufferChunkSealed, storedChunk.Id.Value, chunk: storedChunk));

        await _onChunkSealed(storedChunk, cancellationToken);

        _chunkBuilder.Reset();
        _metrics.ChunkCreated();
        _metrics.UpdateOpenChunkBytes(0);
        _metrics.UpdateMemoryUsage(0);
        _events.Publish(BufferEvent.Create(
            BufferEventType.BufferChunkOpened, _chunkBuilder.ChunkId.Value));
    }

    private async ValueTask<BufferWriteResult?> HandleBackpressureAsync(
        CancellationToken cancellationToken)
    {
        var droppedOldest = false;
        while (true)
        {
            var diskUsed = _metrics.DiskBytesUsed;
            var memUsed = _chunkBuilder.CurrentBytes;

            var (state, shouldAccept) = _backpressure.Evaluate(diskUsed, memUsed);
            var previousState = (BufferState)Volatile.Read(ref _lastState);

            if (state != previousState)
            {
                Volatile.Write(ref _lastState, (int)state);
                _metrics.UpdateState(state);

                if (state >= BufferState.Pressured && previousState < BufferState.Pressured)
                {
                    _events.Publish(BufferEvent.Create(BufferEventType.BufferPressureEntered, detail: state.ToString()));
                }
                else if (state < BufferState.Pressured && previousState >= BufferState.Pressured)
                {
                    _events.Publish(BufferEvent.Create(BufferEventType.BufferPressureExited, detail: state.ToString()));
                }
            }

            if (shouldAccept)
            {
                return droppedOldest
                    ? new BufferWriteResult(BufferWriteStatus.DroppedOldestAndAccepted)
                    : null;
            }

            switch (_options.FullPolicy)
            {
                case BufferFullPolicy.RejectNewest:
                    _metrics.RecordRejected();
                    _events.Publish(BufferEvent.Create(BufferEventType.BufferRecordRejected, detail: "Buffer full"));
                    return new BufferWriteResult(BufferWriteStatus.RejectedBufferFull);

                case BufferFullPolicy.Block:
                    await _spaceAvailable.WaitAsync(cancellationToken);
                    continue;

                case BufferFullPolicy.DropOldest:
                    await _writeLock.WaitAsync(cancellationToken);
                    try
                    {
                        var dropped = await _tryDropOldestChunk(cancellationToken);
                        if (!dropped)
                        {
                            _metrics.RecordRejected();
                            return new BufferWriteResult(BufferWriteStatus.RejectedBufferFull);
                        }

                        _metrics.RecordDropped();
                        droppedOldest = true;
                    }
                    finally
                    {
                        _writeLock.Release();
                    }
                    continue;

                default:
                    return new BufferWriteResult(BufferWriteStatus.RejectedBufferFull);
            }
        }
    }

    private int _lastState;

    public void Dispose()
    {
        _chunkBuilder.Dispose();
        _writeLock.Dispose();
        _spaceAvailable.Dispose();
    }
}