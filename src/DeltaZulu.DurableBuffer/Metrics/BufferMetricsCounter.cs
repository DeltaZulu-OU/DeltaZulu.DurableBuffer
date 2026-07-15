using DeltaZulu.DurableBuffer.Abstractions;

namespace DeltaZulu.DurableBuffer.Metrics;

internal sealed class BufferMetricsCounter : IBufferMetrics
{
    private int _availableChunks;
    private long _chunksCompleted;
    private long _chunksCreated;
    private long _chunksDeadLettered;
    private long _chunksDeadLetterEvicted;
    private long _chunksQuarantined;
    private long _chunksQuarantineEvicted;
    private long _chunksReleased;
    private long _chunksSealed;
    private long _deadLetterBytesLimit;
    private long _deadLetterBytesUsed;
    private long _diskBytesLimit;
    private long _diskBytesUsed;
    private int _dispatcherWaitReason;
    private int _dispatchQueueCapacity;
    private int _dispatchQueueDepth;
    private int _inFlightChunks;
    private int _maxInFlightChunks;
    private long _memoryBytesUsed;
    private long _oldestAvailableChunkAgeTicks = -1;
    private long _oldestChunkAgeTicks = -1;
    private long _oldestDispatchedChunkAgeTicks = -1;
    private long _openChunkBytes;
    private long _quarantineBytesLimit;
    private long _quarantineBytesUsed;
    private long _recordsAccepted;
    private long _recordsDropped;
    private long _recordsRejected;
    private int _sealedChunkCount;
    private int _state;
    internal long DiskBytesUsed => Interlocked.Read(ref _diskBytesUsed);

    public void ChunkCompleted() => Interlocked.Increment(ref _chunksCompleted);

    public void ChunkCreated() => Interlocked.Increment(ref _chunksCreated);

    public void ChunkDeadLettered() => Interlocked.Increment(ref _chunksDeadLettered);

    public void ChunkDeadLetterEvicted() => Interlocked.Increment(ref _chunksDeadLetterEvicted);

    public void ChunkQuarantined() => Interlocked.Increment(ref _chunksQuarantined);

    public void ChunkQuarantineEvicted() => Interlocked.Increment(ref _chunksQuarantineEvicted);

    public void ChunkReleased() => Interlocked.Increment(ref _chunksReleased);

    public void ChunkSealed() => Interlocked.Increment(ref _chunksSealed);

    public void RecordAccepted() => Interlocked.Increment(ref _recordsAccepted);

    public void RecordDropped() => Interlocked.Increment(ref _recordsDropped);

    public void RecordRejected() => Interlocked.Increment(ref _recordsRejected);

    public BufferSnapshot ToSnapshot()
    {
        var ageTicks = Interlocked.Read(ref _oldestChunkAgeTicks);
        var oldestAvailableTicks = Interlocked.Read(ref _oldestAvailableChunkAgeTicks);
        var oldestDispatchedTicks = Interlocked.Read(ref _oldestDispatchedChunkAgeTicks);
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
            ChunksQuarantineEvictedTotal = Interlocked.Read(ref _chunksQuarantineEvicted),
            DispatchQueueCapacity = Volatile.Read(ref _dispatchQueueCapacity),
            DispatchQueueDepth = Volatile.Read(ref _dispatchQueueDepth),
            MaxInFlightChunks = Volatile.Read(ref _maxInFlightChunks),
            InFlightChunks = Volatile.Read(ref _inFlightChunks),
            AvailableChunks = Volatile.Read(ref _availableChunks),
            OldestAvailableChunkAge = oldestAvailableTicks >= 0 ? TimeSpan.FromTicks(oldestAvailableTicks) : null,
            OldestDispatchedChunkAge = oldestDispatchedTicks >= 0 ? TimeSpan.FromTicks(oldestDispatchedTicks) : null,
            DispatcherWaitReason = (DispatchWaitReason)Volatile.Read(ref _dispatcherWaitReason)
        };
    }

    public void UpdateAvailableChunks(int available) => Volatile.Write(ref _availableChunks, available);

    public void UpdateDeadLetterUsage(long bytesUsed, long bytesLimit)
    {
        Interlocked.Exchange(ref _deadLetterBytesUsed, bytesUsed);
        Interlocked.Exchange(ref _deadLetterBytesLimit, bytesLimit);
    }

    public void UpdateDiskUsage(long bytesUsed, long bytesLimit)
    {
        Interlocked.Exchange(ref _diskBytesUsed, bytesUsed);
        Interlocked.Exchange(ref _diskBytesLimit, bytesLimit);
    }

    public void UpdateDispatcherWaitReason(DispatchWaitReason reason) => Volatile.Write(ref _dispatcherWaitReason, (int)reason);

    public void UpdateDispatchQueueCapacity(int capacity) => Volatile.Write(ref _dispatchQueueCapacity, capacity);

    public void UpdateDispatchQueueDepth(int depth) => Volatile.Write(ref _dispatchQueueDepth, depth);

    public void UpdateInFlightChunks(int inFlight) => Volatile.Write(ref _inFlightChunks, inFlight);

    public void UpdateMaxInFlightChunks(int maxInFlight) => Volatile.Write(ref _maxInFlightChunks, maxInFlight);

    public void UpdateMemoryUsage(long bytesUsed) => Interlocked.Exchange(ref _memoryBytesUsed, bytesUsed);

    public void UpdateOldestAvailableChunkAge(TimeSpan? age) => Interlocked.Exchange(ref _oldestAvailableChunkAgeTicks, age?.Ticks ?? -1);

    public void UpdateOldestChunkAge(TimeSpan? age) => Interlocked.Exchange(ref _oldestChunkAgeTicks, age?.Ticks ?? -1);

    public void UpdateOldestDispatchedChunkAge(TimeSpan? age) => Interlocked.Exchange(ref _oldestDispatchedChunkAgeTicks, age?.Ticks ?? -1);

    public void UpdateOpenChunkBytes(long bytes) => Interlocked.Exchange(ref _openChunkBytes, bytes);

    public void UpdateQuarantineUsage(long bytesUsed, long bytesLimit)
    {
        Interlocked.Exchange(ref _quarantineBytesUsed, bytesUsed);
        Interlocked.Exchange(ref _quarantineBytesLimit, bytesLimit);
    }

    public void UpdateSealedChunkCount(int count) => Volatile.Write(ref _sealedChunkCount, count);

    public void UpdateState(BufferState state) => Volatile.Write(ref _state, (int)state);

    internal void AddDeadLetterBytes(long bytes) => Interlocked.Add(ref _deadLetterBytesUsed, bytes);

    internal void AddDiskBytes(long bytes) => Interlocked.Add(ref _diskBytesUsed, bytes);

    internal void AddQuarantineBytes(long bytes) => Interlocked.Add(ref _quarantineBytesUsed, bytes);
}
