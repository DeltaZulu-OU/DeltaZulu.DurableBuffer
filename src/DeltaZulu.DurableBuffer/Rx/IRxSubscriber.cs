namespace DeltaZulu.DurableBuffer.Rx;

public interface IRxSubscriber<in T>
{
    void OnSubscribe(IRxSubscription subscription);

    ValueTask OnNextAsync(T item, CancellationToken cancellationToken = default);

    void OnCompleted(RxCompletion completion);
}
