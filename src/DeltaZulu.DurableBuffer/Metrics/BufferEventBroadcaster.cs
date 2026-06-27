using System.Collections.Immutable;

namespace DeltaZulu.DurableBuffer.Metrics;

internal sealed class BufferEventBroadcaster : IObservable<BufferEvent>
{
    private ImmutableList<IObserver<BufferEvent>> _observers =
        ImmutableList<IObserver<BufferEvent>>.Empty;

    public IDisposable Subscribe(IObserver<BufferEvent> observer)
    {
        ArgumentNullException.ThrowIfNull(observer);

        ImmutableList<IObserver<BufferEvent>> snapshot;
        ImmutableList<IObserver<BufferEvent>> updated;

        do
        {
            snapshot = Volatile.Read(ref _observers);
            updated = snapshot.Add(observer);
        }
        while (!ReferenceEquals(
            Interlocked.CompareExchange(ref _observers, updated, snapshot),
            snapshot));

        return new Unsubscriber(this, observer);
    }

    public void Publish(BufferEvent evt)
    {
        foreach (var observer in Volatile.Read(ref _observers))
        {
            try
            {
                observer.OnNext(evt);
            }
            catch
            {
                // Observer faults must not break the buffer.
            }
        }
    }

    public void Complete()
    {
        var observers = Interlocked.Exchange(
            ref _observers,
            ImmutableList<IObserver<BufferEvent>>.Empty);

        foreach (var observer in observers)
        {
            try
            {
                observer.OnCompleted();
            }
            catch
            {
                // Observer completion faults must not break shutdown.
            }
        }
    }

    private sealed class Unsubscriber(
        BufferEventBroadcaster parent,
        IObserver<BufferEvent> observer) : IDisposable
    {
        public void Dispose()
        {
            ImmutableList<IObserver<BufferEvent>> snapshot;
            ImmutableList<IObserver<BufferEvent>> updated;

            do
            {
                snapshot = Volatile.Read(ref parent._observers);
                updated = snapshot.Remove(observer);

                if (ReferenceEquals(snapshot, updated))
                {
                    return;
                }
            }
            while (!ReferenceEquals(
                Interlocked.CompareExchange(ref parent._observers, updated, snapshot),
                snapshot));
        }
    }
}