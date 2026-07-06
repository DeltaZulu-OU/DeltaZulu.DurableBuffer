namespace DeltaZulu.DurableBuffer.Rx;

public interface IRxSubscription : IAsyncDisposable
{
    ValueTask RequestAsync(long count, CancellationToken cancellationToken = default);

    ValueTask CancelAsync(CancellationToken cancellationToken = default);
}
