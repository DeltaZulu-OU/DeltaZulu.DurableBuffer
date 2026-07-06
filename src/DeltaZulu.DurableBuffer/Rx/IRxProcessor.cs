namespace DeltaZulu.DurableBuffer.Rx;

public interface IRxProcessor<TIn, TOut> : IRxSubscriber<TIn>, IRxPublisher<TOut>
{
}
