using DeltaZulu.DurableBuffer.Metrics;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class BufferEventBroadcasterTests
{
    private sealed class TestObserver : IObserver<BufferEvent>
    {
        public List<BufferEvent> Events { get; } = [];
        public bool Completed { get; private set; }
        public Exception? Error { get; private set; }

        public void OnCompleted() => Completed = true;
        public void OnError(Exception error) => Error = error;
        public void OnNext(BufferEvent value) => Events.Add(value);
    }

    [TestMethod]
    public void Subscribe_ReceivesPublishedEvents()
    {
        var broadcaster = new BufferEventBroadcaster();
        var observer = new TestObserver();
        broadcaster.Subscribe(observer);

        broadcaster.Publish(BufferEvent.Create(BufferEventType.BufferStarted));

        WaitFor(() => observer.Events.Count == 1);
        Assert.HasCount(1, observer.Events);
        Assert.AreEqual(BufferEventType.BufferStarted, observer.Events[0].EventType);
    }

    [TestMethod]
    public void Subscribe_MultipleObservers_AllReceive()
    {
        var broadcaster = new BufferEventBroadcaster();
        var obs1 = new TestObserver();
        var obs2 = new TestObserver();
        broadcaster.Subscribe(obs1);
        broadcaster.Subscribe(obs2);

        broadcaster.Publish(BufferEvent.Create(BufferEventType.BufferStopped));

        WaitFor(() => obs1.Events.Count == 1 && obs2.Events.Count == 1);
        Assert.HasCount(1, obs1.Events);
        Assert.HasCount(1, obs2.Events);
    }

    [TestMethod]
    public void Unsubscribe_StopsReceivingEvents()
    {
        var broadcaster = new BufferEventBroadcaster();
        var observer = new TestObserver();
        var subscription = broadcaster.Subscribe(observer);

        broadcaster.Publish(BufferEvent.Create(BufferEventType.BufferStarted));
        WaitFor(() => observer.Events.Count == 1);
        subscription.Dispose();
        broadcaster.Publish(BufferEvent.Create(BufferEventType.BufferStopped));

        Thread.Sleep(100);
        Assert.HasCount(1, observer.Events);
    }

    [TestMethod]
    public void Complete_NotifiesAllObservers()
    {
        var broadcaster = new BufferEventBroadcaster();
        var obs1 = new TestObserver();
        var obs2 = new TestObserver();
        broadcaster.Subscribe(obs1);
        broadcaster.Subscribe(obs2);

        broadcaster.Complete();

        WaitFor(() => obs1.Completed && obs2.Completed);
        Assert.IsTrue(obs1.Completed);
        Assert.IsTrue(obs2.Completed);
    }

    [TestMethod]
    public void Publish_FaultyObserver_DoesNotBreakOthers()
    {
        var broadcaster = new BufferEventBroadcaster();
        var faultyObserver = new FaultyObserver();
        var goodObserver = new TestObserver();
        broadcaster.Subscribe(faultyObserver);
        broadcaster.Subscribe(goodObserver);

        broadcaster.Publish(BufferEvent.Create(BufferEventType.BufferStarted));

        WaitFor(() => goodObserver.Events.Count == 1);
        Assert.HasCount(1, goodObserver.Events);
    }

    private static void WaitFor(Func<bool> predicate)
    {
        Assert.IsTrue(SpinWait.SpinUntil(predicate, TimeSpan.FromSeconds(2)));
    }

    private sealed class FaultyObserver : IObserver<BufferEvent>
    {
        public void OnCompleted() { }
        public void OnError(Exception error) { }
        public void OnNext(BufferEvent value) => throw new InvalidOperationException("Observer fault");
    }
}
