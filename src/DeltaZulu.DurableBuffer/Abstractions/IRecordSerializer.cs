namespace DeltaZulu.DurableBuffer.Abstractions;

public interface IRecordSerializer<in T>
{
    ReadOnlyMemory<byte> Serialize(T record);
}
