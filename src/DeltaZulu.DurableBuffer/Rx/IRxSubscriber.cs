namespace DeltaZulu.DurableBuffer.Rx;

public interface IRxSubscriber<in T>
{
    void OnCompleted(RxCompletion completion);

    ValueTask OnNextAsync(T item, CancellationToken cancellationToken = default);

    void OnSubscribe(IRxSubscription subscription);
}
