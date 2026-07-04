using DeltaZulu.DurableBuffer.Abstractions;

namespace DeltaZulu.DurableBuffer.Metrics;

internal sealed class BufferMetricsCounter : IBufferMetrics
{
    private long _recordsAccepted;
    private long _recordsRejected;
    private long _recordsDropped;
    private long _chunksCreated;
    private long _chunksSealed;
    private long _chunksCompleted;
    private long _chunksReleased;
    private long _chunksDeadLettered;
    private long _chunksQuarantined;
    private long _chunksDeadLetterEvicted;
    private long _chunksQuarantineEvicted;
    private int _state;
    private long _diskBytesUsed;
    private long _diskBytesLimit;
    private long _deadLetterBytesUsed;
    private long _deadLetterBytesLimit;
    private long _quarantineBytesUsed;
    private long _quarantineBytesLimit;
    private long _memoryBytesUsed;
    private long _openChunkBytes;
    private int _sealedChunkCount;
    private long _oldestChunkAgeTicks = -1;

    public void RecordAccepted() => Interlocked.Increment(ref _recordsAccepted);

    public void RecordRejected() => Interlocked.Increment(ref _recordsRejected);

    public void RecordDropped() => Interlocked.Increment(ref _recordsDropped);

    public void ChunkCreated() => Interlocked.Increment(ref _chunksCreated);

    public void ChunkSealed() => Interlocked.Increment(ref _chunksSealed);

    public void ChunkCompleted() => Interlocked.Increment(ref _chunksCompleted);

    public void ChunkReleased() => Interlocked.Increment(ref _chunksReleased);

    public void ChunkDeadLettered() => Interlocked.Increment(ref _chunksDeadLettered);

    public void ChunkQuarantined() => Interlocked.Increment(ref _chunksQuarantined);

    public void ChunkDeadLetterEvicted() => Interlocked.Increment(ref _chunksDeadLetterEvicted);

    public void ChunkQuarantineEvicted() => Interlocked.Increment(ref _chunksQuarantineEvicted);

    public void UpdateState(BufferState state) => Volatile.Write(ref _state, (int)state);

    public void UpdateDiskUsage(long bytesUsed, long bytesLimit)
    {
        Interlocked.Exchange(ref _diskBytesUsed, bytesUsed);
        Interlocked.Exchange(ref _diskBytesLimit, bytesLimit);
    }

    public void UpdateDeadLetterUsage(long bytesUsed, long bytesLimit)
    {
        Interlocked.Exchange(ref _deadLetterBytesUsed, bytesUsed);
        Interlocked.Exchange(ref _deadLetterBytesLimit, bytesLimit);
    }

    public void UpdateQuarantineUsage(long bytesUsed, long bytesLimit)
    {
        Interlocked.Exchange(ref _quarantineBytesUsed, bytesUsed);
        Interlocked.Exchange(ref _quarantineBytesLimit, bytesLimit);
    }

    public void UpdateMemoryUsage(long bytesUsed) => Interlocked.Exchange(ref _memoryBytesUsed, bytesUsed);

    public void UpdateOpenChunkBytes(long bytes) => Interlocked.Exchange(ref _openChunkBytes, bytes);

    public void UpdateSealedChunkCount(int count) => Volatile.Write(ref _sealedChunkCount, count);

    public void UpdateOldestChunkAge(TimeSpan? age) => Interlocked.Exchange(ref _oldestChunkAgeTicks, age?.Ticks ?? -1);

    internal long DiskBytesUsed => Interlocked.Read(ref _diskBytesUsed);

    internal void AddDiskBytes(long bytes) => Interlocked.Add(ref _diskBytesUsed, bytes);

    internal void AddDeadLetterBytes(long bytes) => Interlocked.Add(ref _deadLetterBytesUsed, bytes);

    internal void AddQuarantineBytes(long bytes) => Interlocked.Add(ref _quarantineBytesUsed, bytes);

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
            OldestChunkAge = ageTicks >= 0 ? TimeSpan.FromTicks(ageTicks) : null,
            RecordsAcceptedTotal = Interlocked.Read(ref _recordsAccepted),
            RecordsRejectedTotal = Interlocked.Read(ref _recordsRejected),
            RecordsDroppedTotal = Interlocked.Read(ref _recordsDropped),
            ChunksCompletedTotal = Interlocked.Read(ref _chunksCompleted),
            ChunksReleasedTotal = Interlocked.Read(ref _chunksReleased),
            ChunksDeadLetteredTotal = Interlocked.Read(ref _chunksDeadLettered),
            DeadLetterBytesUsed = Interlocked.Read(ref _deadLetterBytesUsed),
            DeadLetterBytesLimit = Interlocked.Read(ref _deadLetterBytesLimit),
            QuarantineBytesUsed = Interlocked.Read(ref _quarantineBytesUsed),
            QuarantineBytesLimit = Interlocked.Read(ref _quarantineBytesLimit),
            ChunksDeadLetterEvictedTotal = Interlocked.Read(ref _chunksDeadLetterEvicted),
            ChunksQuarantineEvictedTotal = Interlocked.Read(ref _chunksQuarantineEvicted)
        };
    }
}