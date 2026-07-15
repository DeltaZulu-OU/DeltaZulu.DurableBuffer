namespace DeltaZulu.DurableBuffer.Abstractions;

public enum BufferWriteStatus
{
    Accepted,
    RejectedBufferFull,
    RejectedRecordTooLarge,
    RejectedStopping,
    DroppedOldestAndAccepted
}
