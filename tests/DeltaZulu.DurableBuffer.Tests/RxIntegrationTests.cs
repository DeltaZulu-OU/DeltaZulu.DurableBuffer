using DeltaZulu.DurableBuffer.Chunks;
using DeltaZulu.DurableBuffer.Configuration;
using DeltaZulu.DurableBuffer.Metrics;
using DeltaZulu.DurableBuffer.Abstractions;
using DeltaZulu.DurableBuffer.Rx;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class RxIntegrationTests
{
    private string _storagePath = null!;

    [TestInitialize]
    public void Setup() => _storagePath = Path.Combine(Path.GetTempPath(), $"dz_rx_{Guid.NewGuid():N}");

    [TestCleanup]
    public void Cleanup()
    {
        try { Directory.Delete(_storagePath, true); }
        catch { }
    }

    [TestMethod]
    public async Task RxEvents_SubscribeSink_ReceivesLifecycleEvents()
    {
        var options = new DurableBufferOptions
        {
            StoragePath = _storagePath,
            MaxChunkRecords = 10,
            MaxChunkBytes = 1024 * 1024,
            MaxChunkAge = TimeSpan.FromMinutes(1)
        };

        var sink = new BufferEventSink();

        await using var host = new DurableBufferHost<string>(options, new JsonRecordSerializer<string>());
        using var sub = host.RxEvents.Subscribe(sink);

        await host.StartAsync(TestContext.CancellationToken);
        await host.StopAsync(TestContext.CancellationToken);

        Assert.IsTrue(SpinWait.SpinUntil(() => sink.Events.Any(e => e.EventType == BufferEventType.BufferStarted), TimeSpan.FromSeconds(2)));
        Assert.IsTrue(SpinWait.SpinUntil(() => sink.Events.Any(e => e.EventType == BufferEventType.BufferStopped), TimeSpan.FromSeconds(2)));
    }

    [TestMethod]
    public async Task RxChunks_RequestControlsDelivery()
    {
        var options = new DurableBufferOptions
        {
            StoragePath = _storagePath,
            MaxChunkRecords = 1,
            MaxChunkBytes = 4096,
            MaxChunkAge = TimeSpan.FromMinutes(1)
        };

        await using var host = new DurableBufferHost<string>(options, new JsonRecordSerializer<string>());
        await host.StartAsync(TestContext.CancellationToken);

        var subscriber = new ChunkSubscriber(host.Reader);
        await using var subscription = host.RxChunks.Subscribe(subscriber);

        await subscription.RequestAsync(1, TestContext.CancellationToken);

        await host.Writer.WriteAsync("one", TestContext.CancellationToken);
        await host.Writer.WriteAsync("two", TestContext.CancellationToken);

        await subscriber.FirstReceived.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.CancellationToken);
        await Task.Delay(200, TestContext.CancellationToken);
        Assert.IsFalse(subscriber.SecondReceived.Task.IsCompleted);

        await subscription.RequestAsync(1, TestContext.CancellationToken);
        await subscriber.SecondReceived.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.CancellationToken);

        IReadOnlyList<RxSubscriptionSnapshot> snapshots = [];
        Assert.IsTrue(SpinWait.SpinUntil(() =>
        {
            snapshots = host.RxChunkDiagnostics.GetSubscriptionSnapshots();
            return snapshots.Count == 1 && snapshots[0].DeliveredTotal >= 2;
        }, TimeSpan.FromSeconds(2)));
        Assert.AreEqual(1, snapshots.Count);
        Assert.IsGreaterThanOrEqualTo(2, snapshots[0].DeliveredTotal);

        await host.StopAsync(TestContext.CancellationToken);
    }

    [TestMethod]
    public async Task RxPublisher_AsyncEnumerableRoundTrip_Works()
    {
        var source = GetValues();
        var publisher = source.ToRxPublisher();

        var values = new List<int>();
        await foreach (var item in publisher.ToAsyncEnumerable())
        {
            values.Add(item);
        }

        CollectionAssert.AreEqual(new[] { 1, 2, 3 }, values);
    }

    private static async IAsyncEnumerable<int> GetValues()
    {
        yield return 1;
        await Task.Yield();
        yield return 2;
        await Task.Yield();
        yield return 3;
    }

    private sealed class BufferEventSink : IRxEventSink<BufferEvent>
    {
        public List<BufferEvent> Events { get; } = [];

        public void OnEvent(BufferEvent evt)
        {
            lock (Events)
            {
                Events.Add(evt);
            }
        }

        public void OnCompleted(RxCompletion completion)
        {
        }
    }

    private sealed class ChunkSubscriber(IDurableBufferReader reader) : IRxSubscriber<StoredChunk>
    {
        private int _count;

        public TaskCompletionSource FirstReceived { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource SecondReceived { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void OnSubscribe(IRxSubscription subscription)
        {
        }

        public async ValueTask OnNextAsync(StoredChunk item, CancellationToken cancellationToken = default)
        {
            var count = Interlocked.Increment(ref _count);
            if (count == 1)
            {
                FirstReceived.TrySetResult();
            }
            else if (count == 2)
            {
                SecondReceived.TrySetResult();
            }

            await reader.CompleteAsync(item, cancellationToken);
        }

        public void OnCompleted(RxCompletion completion)
        {
        }
    }

    public TestContext TestContext { get; set; }
}
