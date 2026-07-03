using DeltaZulu.DurableBuffer.Chunks;

namespace DeltaZulu.DurableBuffer.Metrics;

public enum BufferEventType
{
    BufferStarted,
    BufferStopped,
    BufferRecordAccepted,
    BufferRecordRejected,
    BufferChunkOpened,
    BufferChunkSealed,
    BufferChunkDispatchStarted,
    BufferChunkDispatchSucceeded,
    BufferChunkDispatchFailed,
    BufferChunkRetryScheduled,
    BufferChunkDeadLettered,
    BufferChunkDropped,
    BufferPressureEntered,
    BufferPressureExited,
    BufferRecoveryStarted,
    BufferRecoveryCompleted,
    BufferFileQuarantined,
    BufferDeadLetterEvicted,
    BufferQuarantineEvicted
}

public sealed record BufferEvent
{
    public required BufferEventType EventType { get; init; }
    public required DateTimeOffset TimestampUtc { get; init; }
    public string? ChunkId { get; init; }
    public string? Detail { get; init; }
    public StoredChunk? Chunk { get; init; }

    public static BufferEvent Create(
        BufferEventType eventType,
        string? chunkId = null,
        string? detail = null,
        StoredChunk? chunk = null) =>
        new() {
            EventType = eventType,
            TimestampUtc = DateTimeOffset.UtcNow,
            ChunkId = chunkId,
            Detail = detail,
            Chunk = chunk
        };
}