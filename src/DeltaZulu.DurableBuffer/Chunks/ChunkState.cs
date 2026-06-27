namespace DeltaZulu.DurableBuffer.Chunks;

public enum ChunkState
{
    Open,
    Sealing,
    Sealed,
    Dispatchable,
    Dispatching,
    Delivered,
    Deleted,
    RetryScheduled,
    DeadLettered,
    Rejected,
    Corrupt,
    Quarantined
}