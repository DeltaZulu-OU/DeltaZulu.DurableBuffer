using System.Collections.Concurrent;
using System.Threading.Channels;
using DeltaZulu.DurableBuffer.Chunks;
using DeltaZulu.DurableBuffer.Metrics;

namespace DeltaZulu.DurableBuffer.Rx;

internal sealed class RxChunkPublisher : IRxPublisher<StoredChunk>, IRxDispatchDiagnostics<StoredChunk>, IDisposable
{
    private readonly ChannelReader<StoredChunk> _reader;
    private readonly BufferEventBroadcaster _events;
    private readonly object _sync = new();
    private long _nextSubscriptionId;
    private bool _disposed;
    private RxChunkSubscription? _activeSubscription;

    public RxChunkPublisher(ChannelReader<StoredChunk> reader, BufferEventBroadcaster events)
    {
        _reader = reader;
        _events = events;
    }

    public IRxSubscription Subscribe(IRxSubscriber<StoredChunk> subscriber)
    {
        ArgumentNullException.ThrowIfNull(subscriber);

        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_activeSubscription is { IsTerminal: false })
            {
                throw new InvalidOperationException("Only one active chunk subscription is supported.");
            }

            var id = Interlocked.Increment(ref _nextSubscriptionId);
            var subscription = new RxChunkSubscription(id, subscriber, _reader, _events, OnSubscriptionTerminal);
            _activeSubscription = subscription;
            subscriber.OnSubscribe(subscription);
            subscription.Start();
            return subscription;
        }
    }

    public IReadOnlyList<RxSubscriptionSnapshot> GetSubscriptionSnapshots()
    {
        lock (_sync)
        {
            if (_activeSubscription is null)
            {
                return [];
            }

            return [_activeSubscription.ToSnapshot()];
        }
    }

    public IReadOnlyList<RxDispatchSnapshot<StoredChunk>> GetDispatchSnapshots()
    {
        lock (_sync)
        {
            if (_activeSubscription is null)
            {
                return [];
            }

            return _activeSubscription.GetDispatchSnapshots();
        }
    }

    private void OnSubscriptionTerminal(RxChunkSubscription subscription)
    {
        lock (_sync)
        {
            if (ReferenceEquals(_activeSubscription, subscription))
            {
                _activeSubscription = null;
            }
        }
    }

    public void Dispose()
    {
        RxChunkSubscription? subscription;

        lock (_sync)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            subscription = _activeSubscription;
        }

        if (subscription is not null)
        {
            subscription.CancelAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    private sealed class RxChunkSubscription : IRxSubscription
    {
        private readonly long _id;
        private readonly IRxSubscriber<StoredChunk> _subscriber;
        private readonly ChannelReader<StoredChunk> _reader;
        private readonly BufferEventBroadcaster _events;
        private readonly Action<RxChunkSubscription> _onTerminal;
        private readonly CancellationTokenSource _cts = new();
        private readonly SemaphoreSlim _demandSignal = new(0, int.MaxValue);
        private readonly ConcurrentDictionary<string, int> _attemptCounts = new();
        private readonly ConcurrentDictionary<string, RxDispatchSnapshot<StoredChunk>> _inFlight = new();
        private readonly string _subscriberType;
        private readonly DateTimeOffset _subscribedUtc;

        private long _requestedTotal;
        private long _deliveredTotal;
        private long _currentDemand;
        private long _chunkSequence;
        private long _inFlightCount;
        private long _lastSignalTicks;
        private long _completedTicks;
        private long _cancelledTicks;
        private int _state;

        public RxChunkSubscription(
            long id,
            IRxSubscriber<StoredChunk> subscriber,
            ChannelReader<StoredChunk> reader,
            BufferEventBroadcaster events,
            Action<RxChunkSubscription> onTerminal)
        {
            _id = id;
            _subscriber = subscriber;
            _reader = reader;
            _events = events;
            _onTerminal = onTerminal;
            _subscriberType = subscriber.GetType().FullName ?? subscriber.GetType().Name;
            _subscribedUtc = DateTimeOffset.UtcNow;
            Interlocked.Exchange(ref _lastSignalTicks, _subscribedUtc.UtcTicks);

            _events.Publish(BufferEvent.Create(
                BufferEventType.BufferRxSubscriptionStarted,
                detail: $"SubscriptionId={_id};SubscriberType={_subscriberType}"));
        }

        public bool IsTerminal => Volatile.Read(ref _state) != 0;

        public void Start() => _ = Task.Run(PumpAsync);

        public ValueTask RequestAsync(long count, CancellationToken cancellationToken = default)
        {
            if (count <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(count), "Requested count must be greater than zero.");
            }

            if (IsTerminal)
            {
                return ValueTask.CompletedTask;
            }

            cancellationToken.ThrowIfCancellationRequested();
            Interlocked.Add(ref _requestedTotal, count);
            Interlocked.Add(ref _currentDemand, count);
            Interlocked.Exchange(ref _lastSignalTicks, DateTimeOffset.UtcNow.UtcTicks);

            try { _demandSignal.Release(); }
            catch (SemaphoreFullException) { }

            return ValueTask.CompletedTask;
        }

        public ValueTask CancelAsync(CancellationToken cancellationToken = default)
        {
            if (Interlocked.Exchange(ref _state, 2) == 0)
            {
                Interlocked.Exchange(ref _cancelledTicks, DateTimeOffset.UtcNow.UtcTicks);
                _cts.Cancel();
                _events.Publish(BufferEvent.Create(
                    BufferEventType.BufferRxSubscriptionCancelled,
                    detail: $"SubscriptionId={_id}"));
                _onTerminal(this);
            }

            return ValueTask.CompletedTask;
        }

        public async ValueTask DisposeAsync() => await CancelAsync();

        public RxSubscriptionSnapshot ToSnapshot()
        {
            var lastSignalTicks = Interlocked.Read(ref _lastSignalTicks);
            return new RxSubscriptionSnapshot(
                _id,
                _subscriberType,
                _subscribedUtc,
                Interlocked.Read(ref _requestedTotal),
                Interlocked.Read(ref _deliveredTotal),
                ReadTimestamp(ref _cancelledTicks),
                ReadTimestamp(ref _completedTicks),
                new DateTimeOffset(lastSignalTicks, TimeSpan.Zero),
                Interlocked.Read(ref _currentDemand),
                Interlocked.Read(ref _inFlightCount));
        }

        public IReadOnlyList<RxDispatchSnapshot<StoredChunk>> GetDispatchSnapshots() => _inFlight.Values.ToList();

        private async Task PumpAsync()
        {
            var token = _cts.Token;

            try
            {
                while (await _reader.WaitToReadAsync(token))
                {
                    while (Interlocked.Read(ref _currentDemand) <= 0)
                    {
                        await _demandSignal.WaitAsync(token);
                    }

                    if (!_reader.TryRead(out var chunk))
                    {
                        continue;
                    }

                    if (Interlocked.Read(ref _currentDemand) <= 0)
                    {
                        continue;
                    }

                    Interlocked.Decrement(ref _currentDemand);
                    Interlocked.Exchange(ref _lastSignalTicks, DateTimeOffset.UtcNow.UtcTicks);

                    var sequence = Interlocked.Increment(ref _chunkSequence);
                    var attemptCount = _attemptCounts.AddOrUpdate(chunk.Id.Value, 1, (_, existing) => existing + 1);
                    var dispatch = new RxDispatchSnapshot<StoredChunk>(
                        chunk.Id.Value,
                        sequence,
                        DateTimeOffset.UtcNow,
                        _subscriberType,
                        attemptCount,
                        "InFlight",
                        chunk);
                    _inFlight[chunk.Id.Value] = dispatch;
                    Interlocked.Increment(ref _inFlightCount);

                    try
                    {
                        await _subscriber.OnNextAsync(chunk, token);
                        Interlocked.Increment(ref _deliveredTotal);
                        _events.Publish(BufferEvent.Create(
                            BufferEventType.BufferRxChunkDelivered,
                            chunk.Id.Value,
                            detail: $"SubscriptionId={_id};Sequence={sequence};Attempt={attemptCount}",
                            chunk: chunk));
                    }
                    catch (OperationCanceledException) when (token.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        _events.Publish(BufferEvent.Create(
                            BufferEventType.BufferRxChunkSubscriberFault,
                            chunk.Id.Value,
                            detail: $"SubscriptionId={_id};Error={ex.GetType().Name}",
                            chunk: chunk));
                        CompleteInternal(RxCompletion.Failure(ex));
                        return;
                    }
                    finally
                    {
                        _inFlight.TryRemove(chunk.Id.Value, out _);
                        Interlocked.Decrement(ref _inFlightCount);
                        Interlocked.Exchange(ref _lastSignalTicks, DateTimeOffset.UtcNow.UtcTicks);
                    }
                }

                CompleteInternal(RxCompletion.Success);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                CompleteInternal(RxCompletion.Success);
            }
            catch (Exception ex)
            {
                CompleteInternal(RxCompletion.Failure(ex));
            }
        }

        private void CompleteInternal(RxCompletion completion)
        {
            if (Interlocked.CompareExchange(ref _state, 1, 0) != 0)
            {
                return;
            }

            Interlocked.Exchange(ref _completedTicks, DateTimeOffset.UtcNow.UtcTicks);
            Interlocked.Exchange(ref _lastSignalTicks, DateTimeOffset.UtcNow.UtcTicks);

            try { _subscriber.OnCompleted(completion); }
            catch { }

            _events.Publish(BufferEvent.Create(
                BufferEventType.BufferRxSubscriptionCompleted,
                detail: $"SubscriptionId={_id};Success={completion.IsSuccess}"));

            _onTerminal(this);
        }

        private static DateTimeOffset? ReadTimestamp(ref long ticks)
        {
            var value = Interlocked.Read(ref ticks);
            return value == 0 ? null : new DateTimeOffset(value, TimeSpan.Zero);
        }
    }
}
