namespace DeltaZulu.DurableBuffer.Abstractions;

public interface IDurableBufferWriter<T>
{
    ValueTask<BufferWriteResult> WriteAsync(
        T record,
        CancellationToken cancellationToken = default);

    ValueTask FlushAsync(
        CancellationToken cancellationToken = default);

    BufferSnapshot GetSnapshot();
}