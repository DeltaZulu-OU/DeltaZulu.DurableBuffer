namespace DeltaZulu.DurableBuffer.Chunks;

public enum ChunkState
{
    Open,
    Sealing,
    Sealed,
    Completed,
    Deleted,
    DeadLettered,
    Rejected,
    Corrupt,
    Quarantined
}