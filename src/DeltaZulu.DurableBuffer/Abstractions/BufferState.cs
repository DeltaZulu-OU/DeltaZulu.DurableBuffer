namespace DeltaZulu.DurableBuffer.Abstractions;

public enum BufferState
{
    Healthy,
    Degraded,
    Pressured,
    Full
}