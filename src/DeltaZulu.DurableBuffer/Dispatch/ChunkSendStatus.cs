namespace DeltaZulu.DurableBuffer.Dispatch;

public enum ChunkSendStatus
{
    Success,
    TransientFailure,
    PermanentFailure
}