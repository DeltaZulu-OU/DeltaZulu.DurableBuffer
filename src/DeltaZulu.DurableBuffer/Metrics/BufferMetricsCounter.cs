using DeltaZulu.DurableBuffer.Abstractions;

namespace DeltaZulu.DurableBuffer.Metrics;

internal sealed class BufferMetricsCounter : IBufferMetrics
{
    private long _recordsAccepted;
    private long _recordsRejected;
    private long _recordsDropped;
    private long _chunksCreated;
    private long _chunksSealed;
    private long _chunksSent;
    private long _chunksDelivered;
    private long _chunksFailed;
    private long _chunksRetried;
    private long _chunksDeadLettered;
    private long _chunksQuarantined;
    private int _state;
    private long _diskBytesUsed;
    private long _diskBytesLimit;
    private long _memoryBytesUsed;
    private long _openChunkBytes;
    private int _sealedChunkCount;
    private long _oldestChunkAgeTicks = -1;
    private int _retryQueueDepth;

    public void RecordAccepted() => Interlocked.Increment(ref _recordsAccepted);

    public void RecordRejected() => Interlocked.Increment(ref _recordsRejected);

    public void RecordDropped() => Interlocked.Increment(ref _recordsDropped);

    public void ChunkCreated() => Interlocked.Increment(ref _chunksCreated);

    public void ChunkSealed() => Interlocked.Increment(ref _chunksSealed);

    public void ChunkSent() => Interlocked.Increment(ref _chunksSent);

    public void ChunkDelivered() => Interlocked.Increment(ref _chunksDelivered);

    public void ChunkFailed() => Interlocked.Increment(ref _chunksFailed);

    public void ChunkRetried() => Interlocked.Increment(ref _chunksRetried);

    public void ChunkDeadLettered() => Interlocked.Increment(ref _chunksDeadLettered);

    public void ChunkQuarantined() => Interlocked.Increment(ref _chunksQuarantined);

    public void UpdateState(BufferState state) => Volatile.Write(ref _state, (int)state);

    public void UpdateDiskUsage(long bytesUsed, long bytesLimit)
    {
        Interlocked.Exchange(ref _diskBytesUsed, bytesUsed);
        Interlocked.Exchange(ref _diskBytesLimit, bytesLimit);
    }

    public void UpdateMemoryUsage(long bytesUsed) => Interlocked.Exchange(ref _memoryBytesUsed, bytesUsed);

    public void UpdateOpenChunkBytes(long bytes) => Interlocked.Exchange(ref _openChunkBytes, bytes);

    public void UpdateSealedChunkCount(int count) => Volatile.Write(ref _sealedChunkCount, count);

    public void UpdateOldestChunkAge(TimeSpan? age) => Interlocked.Exchange(ref _oldestChunkAgeTicks, age?.Ticks ?? -1);

    public void UpdateRetryQueueDepth(int depth) => Volatile.Write(ref _retryQueueDepth, depth);

    internal long DiskBytesUsed => Interlocked.Read(ref _diskBytesUsed);

    internal void AddDiskBytes(long bytes) => Interlocked.Add(ref _diskBytesUsed, bytes);

    public BufferSnapshot ToSnapshot()
    {
        var ageTicks = Interlocked.Read(ref _oldestChunkAgeTicks);
        return new BufferSnapshot {
            State = (BufferState)Volatile.Read(ref _state),
            DiskBytesUsed = Interlocked.Read(ref _diskBytesUsed),
            DiskBytesLimit = Interlocked.Read(ref _diskBytesLimit),
            MemoryBytesUsed = Interlocked.Read(ref _memoryBytesUsed),
            OpenChunkBytes = Interlocked.Read(ref _openChunkBytes),
            SealedChunkCount = Volatile.Read(ref _sealedChunkCount),
            RetryQueueDepth = Volatile.Read(ref _retryQueueDepth),
            OldestChunkAge = ageTicks >= 0 ? TimeSpan.FromTicks(ageTicks) : null,
            RecordsAcceptedTotal = Interlocked.Read(ref _recordsAccepted),
            RecordsRejectedTotal = Interlocked.Read(ref _recordsRejected),
            RecordsDroppedTotal = Interlocked.Read(ref _recordsDropped),
            ChunksSentTotal = Interlocked.Read(ref _chunksSent),
            ChunksDeliveredTotal = Interlocked.Read(ref _chunksDelivered),
            ChunksFailedTotal = Interlocked.Read(ref _chunksFailed),
            ChunksRetryScheduledTotal = Interlocked.Read(ref _chunksRetried),
            ChunksDeadLetteredTotal = Interlocked.Read(ref _chunksDeadLettered)
        };
    }
}