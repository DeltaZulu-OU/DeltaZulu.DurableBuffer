namespace DeltaZulu.DurableBuffer.Abstractions;

public enum DispatchWaitReason
{
    None = 0,
    NoAvailableChunks = 1,
    DispatchQueueFull = 2,
    InFlightLimitReached = 3,
    Stopping = 4
}
