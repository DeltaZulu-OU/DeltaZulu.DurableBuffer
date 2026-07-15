using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace DeltaZulu.DurableBuffer.Rx;

public static class RxAdapters
{
    public static IAsyncEnumerable<T> ToAsyncEnumerable<T>(this IRxPublisher<T> publisher, int requestBatchSize = 1)
    {
        ArgumentNullException.ThrowIfNull(publisher);

        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(requestBatchSize);

        return ReadAll(publisher, requestBatchSize);
    }

    public static IObservable<TEvent> ToObservable<TEvent>(this IRxEventStream<TEvent> stream)
    {
        ArgumentNullException.ThrowIfNull(stream);
        return new RxEventStreamObservable<TEvent>(stream);
    }

    public static IRxEventStream<TEvent> ToRxEventStream<TEvent>(this IObservable<TEvent> observable)
    {
        ArgumentNullException.ThrowIfNull(observable);
        return new ObservableRxEventStream<TEvent>(observable);
    }

    public static IRxPublisher<T> ToRxPublisher<T>(this IAsyncEnumerable<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        return new AsyncEnumerableRxPublisher<T>(source);
    }

    private static async IAsyncEnumerable<T> ReadAll<T>(
        IRxPublisher<T> publisher,
        int requestBatchSize,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(requestBatchSize * 2) {
            SingleReader = true,
            SingleWriter = false,
            FullMode = BoundedChannelFullMode.Wait
        });

        var subscriber = new AsyncEnumerableSubscriber<T>(channel.Writer, requestBatchSize);
        await using var subscription = publisher.Subscribe(subscriber);
        await subscriber.WaitForSubscriptionAsync(cancellationToken);
        await subscriber.Subscription!.RequestAsync(requestBatchSize, cancellationToken);

        try
        {
            await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return item;
                await subscriber.Subscription.RequestAsync(1, cancellationToken);
            }
        }
        finally
        {
            await subscriber.Subscription.CancelAsync(cancellationToken);
        }
    }

    private sealed class AsyncEnumerableRxPublisher<T>(IAsyncEnumerable<T> source) : IRxPublisher<T>
    {
        public IRxSubscription Subscribe(IRxSubscriber<T> subscriber)
        {
            ArgumentNullException.ThrowIfNull(subscriber);
            var subscription = new Subscription(source, subscriber);
            subscriber.OnSubscribe(subscription);
            subscription.Start();
            return subscription;
        }

        private sealed class Subscription(
            IAsyncEnumerable<T> source,
            IRxSubscriber<T> subscriber) : IRxSubscription
        {
            private readonly CancellationTokenSource _cts = new();
            private readonly SemaphoreSlim _demandSignal = new(0, int.MaxValue);
            private long _demand;
            private int _state;

            public ValueTask CancelAsync(CancellationToken cancellationToken = default)
            {
                if (Interlocked.Exchange(ref _state, 2) == 0)
                {
                    _cts.Cancel();
                }

                return ValueTask.CompletedTask;
            }

            public async ValueTask DisposeAsync() => await CancelAsync();

            public ValueTask RequestAsync(long count, CancellationToken cancellationToken = default)
            {
                ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);

                if (Volatile.Read(ref _state) != 0)
                {
                    return ValueTask.CompletedTask;
                }

                cancellationToken.ThrowIfCancellationRequested();
                Interlocked.Add(ref _demand, count);
                try { _demandSignal.Release(); }
                catch (SemaphoreFullException) { }
                return ValueTask.CompletedTask;
            }

            public void Start() => _ = Task.Run(PumpAsync);

            private async Task PumpAsync()
            {
                var token = _cts.Token;
                try
                {
                    await foreach (var item in source.WithCancellation(token))
                    {
                        while (Interlocked.Read(ref _demand) <= 0)
                        {
                            await _demandSignal.WaitAsync(token);
                        }

                        Interlocked.Decrement(ref _demand);
                        await subscriber.OnNextAsync(item, token);
                    }

                    if (Interlocked.CompareExchange(ref _state, 1, 0) == 0)
                    {
                        subscriber.OnCompleted(RxCompletion.Success);
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    if (Interlocked.CompareExchange(ref _state, 1, 2) == 2)
                    {
                        subscriber.OnCompleted(RxCompletion.Success);
                    }
                }
                catch (Exception ex)
                {
                    if (Interlocked.CompareExchange(ref _state, 1, 0) == 0)
                    {
                        subscriber.OnCompleted(RxCompletion.Failure(ex));
                    }
                }
            }
        }
    }

    private sealed class AsyncEnumerableSubscriber<T>(
        ChannelWriter<T> writer,
        int requestBatchSize) : IRxSubscriber<T>
    {
        private readonly TaskCompletionSource _onSubscribed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _receivedSinceRequest;

        public IRxSubscription? Subscription { get; private set; }

        public void OnCompleted(RxCompletion completion) => writer.TryComplete(completion.Exception);

        public async ValueTask OnNextAsync(T item, CancellationToken cancellationToken = default)
        {
            await writer.WriteAsync(item, cancellationToken);
            _receivedSinceRequest++;
            if (_receivedSinceRequest >= requestBatchSize)
            {
                _receivedSinceRequest = 0;
            }
        }

        public void OnSubscribe(IRxSubscription subscription)
        {
            Subscription = subscription;
            _onSubscribed.TrySetResult();
        }

        public async Task WaitForSubscriptionAsync(CancellationToken cancellationToken) => await _onSubscribed.Task.WaitAsync(cancellationToken);
    }

    private sealed class ObservableRxEventStream<TEvent>(IObservable<TEvent> observable) : IRxEventStream<TEvent>
    {
        public IDisposable Subscribe(IRxEventSink<TEvent> sink)
        {
            ArgumentNullException.ThrowIfNull(sink);
            return observable.Subscribe(new Observer(sink));
        }

        private sealed class Observer(IRxEventSink<TEvent> sink) : IObserver<TEvent>
        {
            public void OnCompleted() => sink.OnCompleted(RxCompletion.Success);

            public void OnError(Exception error) => sink.OnCompleted(RxCompletion.Failure(error));

            public void OnNext(TEvent value) => sink.OnEvent(value);
        }
    }

    private sealed class RxEventStreamObservable<TEvent>(IRxEventStream<TEvent> stream) : IObservable<TEvent>
    {
        public IDisposable Subscribe(IObserver<TEvent> observer)
        {
            ArgumentNullException.ThrowIfNull(observer);
            return stream.Subscribe(new Sink(observer));
        }

        private sealed class Sink(IObserver<TEvent> observer) : IRxEventSink<TEvent>
        {
            public void OnCompleted(RxCompletion completion)
            {
                if (completion.Exception is not null)
                {
                    observer.OnError(completion.Exception);
                    return;
                }

                observer.OnCompleted();
            }

            public void OnEvent(TEvent evt) => observer.OnNext(evt);
        }
    }
}
