using System;

namespace DeltaZulu.DurableBuffer.Abstractions;

public sealed record BufferSnapshot
{
    public required BufferState State { get; init; }
    public required long DiskBytesUsed { get; init; }
    public required long DiskBytesLimit { get; init; }
    public required long MemoryBytesUsed { get; init; }
    public required long OpenChunkBytes { get; init; }
    public required int SealedChunkCount { get; init; }
    public required TimeSpan? OldestChunkAge { get; init; }
    public required long RecordsAcceptedTotal { get; init; }
    public required long RecordsRejectedTotal { get; init; }
    public required long RecordsDroppedTotal { get; init; }
    public required long ChunksCompletedTotal { get; init; }
    public required long ChunksReleasedTotal { get; init; }
    public required long ChunksDeadLetteredTotal { get; init; }
    public required long DeadLetterBytesUsed { get; init; }
    public required long DeadLetterBytesLimit { get; init; }
    public required long QuarantineBytesUsed { get; init; }
    public required long QuarantineBytesLimit { get; init; }
    public required long ChunksDeadLetterEvictedTotal { get; init; }
    public required long ChunksQuarantineEvictedTotal { get; init; }
    public required int DispatchQueueCapacity { get; init; }
    public required int DispatchQueueDepth { get; init; }
    public required int MaxInFlightChunks { get; init; }
    public required int InFlightChunks { get; init; }
    public required int AvailableChunks { get; init; }
    public required TimeSpan? OldestAvailableChunkAge { get; init; }
    public required TimeSpan? OldestDispatchedChunkAge { get; init; }
    public required DispatchWaitReason DispatcherWaitReason { get; init; }
}