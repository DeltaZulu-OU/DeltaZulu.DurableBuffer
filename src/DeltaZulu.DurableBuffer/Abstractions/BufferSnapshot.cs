namespace DeltaZulu.DurableBuffer.Abstractions;

public sealed record BufferSnapshot
{
    public required BufferState State { get; init; }
    public required long DiskBytesUsed { get; init; }
    public required long DiskBytesLimit { get; init; }
    public required long MemoryBytesUsed { get; init; }
    public required long OpenChunkBytes { get; init; }
    public required int SealedChunkCount { get; init; }
    public required int RetryQueueDepth { get; init; }
    public required TimeSpan? OldestChunkAge { get; init; }
    public required long RecordsAcceptedTotal { get; init; }
    public required long RecordsRejectedTotal { get; init; }
    public required long RecordsDroppedTotal { get; init; }
    public required long ChunksSentTotal { get; init; }
    public required long ChunksDeliveredTotal { get; init; }
    public required long ChunksFailedTotal { get; init; }
    public required long ChunksRetryScheduledTotal { get; init; }
    public required long ChunksDeadLetteredTotal { get; init; }
    public required long DeadLetterBytesUsed { get; init; }
    public required long DeadLetterBytesLimit { get; init; }
    public required long QuarantineBytesUsed { get; init; }
    public required long QuarantineBytesLimit { get; init; }
    public required long ChunksDeadLetterEvictedTotal { get; init; }
    public required long ChunksQuarantineEvictedTotal { get; init; }
}