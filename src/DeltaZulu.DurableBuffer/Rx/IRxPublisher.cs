namespace DeltaZulu.DurableBuffer.Rx;

public interface IRxPublisher<T>
{
    IRxSubscription Subscribe(IRxSubscriber<T> subscriber);
}
