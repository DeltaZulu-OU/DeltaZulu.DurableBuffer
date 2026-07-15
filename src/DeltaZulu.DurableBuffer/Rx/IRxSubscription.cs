namespace DeltaZulu.DurableBuffer.Rx;

public interface IRxSubscription : IAsyncDisposable
{
    ValueTask CancelAsync(CancellationToken cancellationToken = default);

    ValueTask RequestAsync(long count, CancellationToken cancellationToken = default);
}
