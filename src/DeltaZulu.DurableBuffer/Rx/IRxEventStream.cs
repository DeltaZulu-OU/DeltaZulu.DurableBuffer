namespace DeltaZulu.DurableBuffer.Rx;

public interface IRxEventStream<TEvent>
{
    IDisposable Subscribe(IRxEventSink<TEvent> sink);
}

public interface IRxEventSink<in TEvent>
{
    void OnEvent(TEvent evt);

    void OnCompleted(RxCompletion completion);
}
