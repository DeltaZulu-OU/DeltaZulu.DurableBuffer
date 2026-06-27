namespace DeltaZulu.DurableBuffer.Configuration;

public enum BufferFullPolicy
{
    Block,
    RejectNewest,
    DropOldest
}