namespace DeltaZulu.DurableBuffer.Abstractions;

public interface IDurableBuffer<T>
{
    ValueTask FlushAsync(
        CancellationToken cancellationToken = default);

    BufferSnapshot GetSnapshot();

    ValueTask<BufferWriteResult> WriteAsync(
                T record,
        CancellationToken cancellationToken = default);
}
