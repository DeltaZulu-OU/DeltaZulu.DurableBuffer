using System.Collections.Immutable;
using System.Threading.Channels;
using DeltaZulu.DurableBuffer.Rx;

namespace DeltaZulu.DurableBuffer.Metrics;

internal sealed class BufferEventBroadcaster : IObservable<BufferEvent>, IRxEventStream<BufferEvent>
{
    private const int SubscriberQueueCapacity = 256;

    private int _completed;
    private ImmutableList<SubscriberState> _subscribers = ImmutableList<SubscriberState>.Empty;

    public void Complete()
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0)
        {
            return;
        }

        var subscribers = Interlocked.Exchange(ref _subscribers, ImmutableList<SubscriberState>.Empty);
        foreach (var subscriber in subscribers)
        {
            subscriber.Complete(RxCompletion.Success);
        }
    }

    public void Publish(BufferEvent evt)
    {
        foreach (var subscriber in Volatile.Read(ref _subscribers))
        {
            if (subscriber.TryEnqueue(evt))
            {
                continue;
            }

            subscriber.IncrementDropped();
        }
    }

    public IDisposable Subscribe(IObserver<BufferEvent> observer)
    {
        ArgumentNullException.ThrowIfNull(observer);

        return SubscribeCore(
            onEvent: evt => {
                try
                {
                    observer.OnNext(evt);
                }
                catch
                {
                    // Observer faults must not break the buffer.
                }
            },
            onCompleted: completion => {
                try
                {
                    if (completion.Exception is not null)
                    {
                        observer.OnError(completion.Exception);
                    }
                    else
                    {
                        observer.OnCompleted();
                    }
                }
                catch
                {
                    // Observer completion faults must not break shutdown.
                }
            });
    }

    public IDisposable Subscribe(IRxEventSink<BufferEvent> sink)
    {
        ArgumentNullException.ThrowIfNull(sink);

        return SubscribeCore(
            onEvent: evt => {
                try
                {
                    sink.OnEvent(evt);
                }
                catch
                {
                    // Sink faults must not break the buffer.
                }
            },
            onCompleted: completion => {
                try
                {
                    sink.OnCompleted(completion);
                }
                catch
                {
                    // Sink completion faults must not break shutdown.
                }
            });
    }

    private void PublishDropNotice(long droppedCount) => Publish(BufferEvent.Create(
            BufferEventType.BufferRxEventDropped,
            detail: $"Dropped event count reached {droppedCount}"));

    private void Remove(SubscriberState subscriber)
    {
        ImmutableList<SubscriberState> snapshot;
        ImmutableList<SubscriberState> updated;

        do
        {
            snapshot = Volatile.Read(ref _subscribers);
            updated = snapshot.Remove(subscriber);

            if (ReferenceEquals(snapshot, updated))
            {
                return;
            }
        }
        while (!ReferenceEquals(
            Interlocked.CompareExchange(ref _subscribers, updated, snapshot),
            snapshot));
    }

    private IDisposable SubscribeCore(Action<BufferEvent> onEvent, Action<RxCompletion> onCompleted)
    {
        if (Volatile.Read(ref _completed) != 0)
        {
            onCompleted(RxCompletion.Success);
            return EmptyDisposable.Instance;
        }

        var subscriber = new SubscriberState(this, onEvent, onCompleted);

        ImmutableList<SubscriberState> snapshot;
        ImmutableList<SubscriberState> updated;

        do
        {
            snapshot = Volatile.Read(ref _subscribers);
            updated = snapshot.Add(subscriber);
        }
        while (!ReferenceEquals(
            Interlocked.CompareExchange(ref _subscribers, updated, snapshot),
            snapshot));

        subscriber.Start();
        return subscriber;
    }

    private sealed class EmptyDisposable : IDisposable
    {
        public static EmptyDisposable Instance { get; } = new();

        public void Dispose()
        {
        }
    }

    private sealed class SubscriberState(
            BufferEventBroadcaster parent,
        Action<BufferEvent> onEvent,
        Action<RxCompletion> onCompleted) : IDisposable
    {
        private readonly CancellationTokenSource _cts = new();

        private readonly Channel<BufferEvent> _queue = Channel.CreateBounded<BufferEvent>(
            new BoundedChannelOptions(SubscriberQueueCapacity) {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });

        private RxCompletion _completion = RxCompletion.Success;
        private int _disposed;
        private long _dropped;

        public void Complete(RxCompletion completion)
        {
            _completion = completion;
            _queue.Writer.TryComplete(completion.Exception);
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            _cts.Cancel();
            _queue.Writer.TryComplete();
            parent.Remove(this);
        }

        public void IncrementDropped()
        {
            var dropped = Interlocked.Increment(ref _dropped);
            if (dropped % 100 == 0)
            {
                parent.PublishDropNotice(dropped);
            }
        }

        public void Start() => _ = Task.Run(PumpAsync);

        public bool TryEnqueue(BufferEvent evt) => _queue.Writer.TryWrite(evt);

        private async Task PumpAsync()
        {
            try
            {
                await foreach (var evt in _queue.Reader.ReadAllAsync(_cts.Token))
                {
                    onEvent(evt);
                }
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
            }
            finally
            {
                onCompleted(_completion);
                parent.Remove(this);
            }
        }
    }
}
