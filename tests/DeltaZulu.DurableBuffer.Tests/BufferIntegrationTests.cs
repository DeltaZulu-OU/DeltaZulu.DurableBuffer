using System.Text;
using System.Threading.Channels;
using DeltaZulu.DurableBuffer.Abstractions;
using DeltaZulu.DurableBuffer.Chunks;
using DeltaZulu.DurableBuffer.Configuration;
using DeltaZulu.DurableBuffer.Metrics;
using DeltaZulu.DurableBuffer.Storage;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class BufferIntegrationTests
{
    private string _storagePath = null!;

    [TestInitialize]
    public void Setup() => _storagePath = Path.Combine(Path.GetTempPath(), $"dz_integ_{Guid.NewGuid():N}");

    [TestCleanup]
    public void Cleanup()
    {
        try { Directory.Delete(_storagePath, true); }
        catch { }
    }

    private sealed class CompletingConsumer
    {
        public int ConsumedCount;
        public List<StoredChunk> ConsumedChunks { get; } = [];

        public async Task RunAsync(IDurableBufferReader reader, CancellationToken cancellationToken)
        {
            await foreach (var chunk in reader.SealedChunks.ReadAllAsync(cancellationToken))
            {
                Interlocked.Increment(ref ConsumedCount);
                lock (ConsumedChunks)
                {
                    ConsumedChunks.Add(chunk);
                }

                await reader.CompleteAsync(chunk, cancellationToken);
            }
        }
    }

    private sealed class DeadLetteringConsumer
    {
        public async Task RunAsync(IDurableBufferReader reader, CancellationToken cancellationToken)
        {
            await foreach (var chunk in reader.SealedChunks.ReadAllAsync(cancellationToken))
            {
                await reader.DeadLetterAsync(chunk, "simulated failure", cancellationToken);
            }
        }
    }

    [TestMethod]
    public async Task WriteAndConsume_EndToEnd()
    {
        var consumer = new CompletingConsumer();
        var options = new DurableBufferOptions {
            StoragePath = _storagePath,
            MaxChunkRecords = 2,
            MaxChunkBytes = 4096,
            MaxChunkAge = TimeSpan.FromSeconds(30)
        };

        await using var host = new DurableBufferHost<string>(options, new JsonRecordSerializer<string>());
        var consumerTask = Task.Run(() => consumer.RunAsync(host.Reader, TestContext.CancellationToken));
        await host.StartAsync(TestContext.CancellationToken);

        var r1 = await host.Writer.WriteAsync("record1", TestContext.CancellationToken);
        var r2 = await host.Writer.WriteAsync("record2", TestContext.CancellationToken);

        Assert.AreEqual(BufferWriteStatus.Accepted, r1.Status);
        Assert.AreEqual(BufferWriteStatus.Accepted, r2.Status);

        await host.StopAsync(TestContext.CancellationToken);
        await consumerTask.WaitAsync(TimeSpan.FromSeconds(10), TestContext.CancellationToken);

        Assert.IsGreaterThanOrEqualTo(1, consumer.ConsumedCount);
        Assert.AreEqual(0, Directory.Exists(Path.Combine(_storagePath, "sealed"))
            ? Directory.EnumerateFiles(Path.Combine(_storagePath, "sealed"), "*.chunk").Count()
            : 0);
    }

    [TestMethod]
    public async Task Write_FlushOnStop_DeliversPendingChunk()
    {
        var consumer = new CompletingConsumer();
        var options = new DurableBufferOptions {
            StoragePath = _storagePath,
            MaxChunkRecords = 100,
            MaxChunkBytes = 1024 * 1024,
            MaxChunkAge = TimeSpan.FromMinutes(5)
        };

        await using var host = new DurableBufferHost<string>(options, new JsonRecordSerializer<string>());
        var consumerTask = Task.Run(() => consumer.RunAsync(host.Reader, TestContext.CancellationToken));
        await host.StartAsync(TestContext.CancellationToken);

        await host.Writer.WriteAsync("pending1", TestContext.CancellationToken);
        await host.Writer.WriteAsync("pending2", TestContext.CancellationToken);

        await host.StopAsync(TestContext.CancellationToken);
        await consumerTask.WaitAsync(TimeSpan.FromSeconds(10), TestContext.CancellationToken);

        Assert.IsGreaterThanOrEqualTo(1, consumer.ConsumedCount);
    }

    [TestMethod]
    public async Task GetSnapshot_ReflectsState()
    {
        var options = new DurableBufferOptions {
            StoragePath = _storagePath,
            MaxChunkRecords = 100,
            MaxChunkBytes = 1024 * 1024,
            MaxChunkAge = TimeSpan.FromMinutes(5)
        };

        await using var host = new DurableBufferHost<string>(options, new JsonRecordSerializer<string>());
        await host.StartAsync(TestContext.CancellationToken);

        await host.Writer.WriteAsync("test", TestContext.CancellationToken);
        var snapshot = host.Writer.GetSnapshot();

        Assert.IsGreaterThanOrEqualTo(1, snapshot.RecordsAcceptedTotal);
        Assert.IsGreaterThan(0, snapshot.OpenChunkBytes);

        await host.StopAsync(TestContext.CancellationToken);
    }

    [TestMethod]
    public async Task Events_ReceivesStartAndStopEvents()
    {
        var options = new DurableBufferOptions {
            StoragePath = _storagePath,
            MaxChunkRecords = 100,
            MaxChunkBytes = 1024 * 1024,
            MaxChunkAge = TimeSpan.FromMinutes(5)
        };

        var events = new List<BufferEvent>();

        await using var host = new DurableBufferHost<string>(options, new JsonRecordSerializer<string>());
        host.Events.Subscribe(new EventCollector(events));

        await host.StartAsync(TestContext.CancellationToken);
        await host.StopAsync(TestContext.CancellationToken);

        Assert.IsTrue(SpinWait.SpinUntil(
            () => events.Any(e => e.EventType == BufferEventType.BufferStarted),
            TimeSpan.FromSeconds(2)));
        Assert.IsTrue(SpinWait.SpinUntil(
            () => events.Any(e => e.EventType == BufferEventType.BufferStopped),
            TimeSpan.FromSeconds(2)));
        Assert.Contains(e => e.EventType == BufferEventType.BufferStarted, events);
        Assert.Contains(e => e.EventType == BufferEventType.BufferStopped, events);
    }

    [TestMethod]
    public async Task Write_RejectedStopping_AfterStop()
    {
        var options = new DurableBufferOptions {
            StoragePath = _storagePath,
            MaxChunkRecords = 100,
            MaxChunkBytes = 1024 * 1024,
            MaxChunkAge = TimeSpan.FromMinutes(5)
        };

        await using var host = new DurableBufferHost<string>(options, new JsonRecordSerializer<string>());
        await host.StartAsync(TestContext.CancellationToken);
        await host.StopAsync(TestContext.CancellationToken);

        var result = await host.Writer.WriteAsync("too-late", TestContext.CancellationToken);
        Assert.AreEqual(BufferWriteStatus.RejectedStopping, result.Status);
    }

    [TestMethod]
    public async Task Consumer_DeadLetter_MovesToDeadLetter()
    {
        var consumer = new DeadLetteringConsumer();
        var options = new DurableBufferOptions {
            StoragePath = _storagePath,
            MaxChunkRecords = 1,
            MaxChunkBytes = 4096,
            MaxChunkAge = TimeSpan.FromSeconds(30)
        };

        var events = new List<BufferEvent>();

        await using var host = new DurableBufferHost<string>(options, new JsonRecordSerializer<string>());
        host.Events.Subscribe(new EventCollector(events));

        var consumerTask = Task.Run(() => consumer.RunAsync(host.Reader, TestContext.CancellationToken));
        await host.StartAsync(TestContext.CancellationToken);
        await host.Writer.WriteAsync("doomed", TestContext.CancellationToken);
        await host.StopAsync(TestContext.CancellationToken);
        await consumerTask.WaitAsync(TimeSpan.FromSeconds(10), TestContext.CancellationToken);

        Assert.IsTrue(SpinWait.SpinUntil(
            () => events.Any(e => e.EventType == BufferEventType.BufferChunkDeadLettered),
            TimeSpan.FromSeconds(2)));
        Assert.Contains(e => e.EventType == BufferEventType.BufferChunkDeadLettered, events);
        var deadLetterDir = Path.Combine(_storagePath, "deadletter");
        Assert.IsTrue(Directory.Exists(deadLetterDir));
        Assert.IsNotEmpty(Directory.EnumerateFiles(deadLetterDir, "*.chunk"));
    }

    [TestMethod]
    public async Task Consumer_Release_RequeuesChunk()
    {
        var options = new DurableBufferOptions {
            StoragePath = _storagePath,
            MaxChunkRecords = 1,
            MaxChunkBytes = 4096,
            MaxChunkAge = TimeSpan.FromSeconds(30)
        };

        await using var host = new DurableBufferHost<string>(options, new JsonRecordSerializer<string>());
        await host.StartAsync(TestContext.CancellationToken);
        await host.Writer.WriteAsync("retry-me", TestContext.CancellationToken);
        await host.Writer.FlushAsync(TestContext.CancellationToken);

        var first = await host.Reader.SealedChunks.ReadAsync(TestContext.CancellationToken);
        await host.Reader.ReleaseAsync(first, TestContext.CancellationToken);

        var second = await host.Reader.SealedChunks.ReadAsync(TestContext.CancellationToken);
        Assert.AreEqual(first.Id, second.Id);

        await host.Reader.CompleteAsync(second, TestContext.CancellationToken);
        await host.StopAsync(TestContext.CancellationToken);
    }

    [TestMethod]
    public async Task RecordTooLarge_IsRejected()
    {
        var options = new DurableBufferOptions {
            StoragePath = _storagePath,
            MaxChunkRecords = 100,
            MaxChunkBytes = 200,
            MaxChunkAge = TimeSpan.FromMinutes(5)
        };

        await using var host = new DurableBufferHost<string>(options, new JsonRecordSerializer<string>());
        await host.StartAsync(TestContext.CancellationToken);

        var largeRecord = new string('x', 300);
        var result = await host.Writer.WriteAsync(largeRecord, TestContext.CancellationToken);

        Assert.AreEqual(BufferWriteStatus.RejectedRecordTooLarge, result.Status);

        await host.StopAsync(TestContext.CancellationToken);
    }

    [TestMethod]
    public async Task Write_DropOldestPolicy_WritesIncomingRecordAfterDroppingOldestChunk()
    {
        var options = new DurableBufferOptions {
            StoragePath = _storagePath,
            MaxDiskBytes = 1,
            MaxChunkRecords = 100,
            MaxChunkBytes = 4096,
            MaxChunkAge = TimeSpan.FromMinutes(5),
            FullPolicy = BufferFullPolicy.DropOldest
        };
        var store = new FileChunkStore(_storagePath);
        var metrics = new BufferMetricsCounter();
        var events = new BufferEventBroadcaster();
        var chunkChannel = Channel.CreateUnbounded<StoredChunk>();
        var backpressure = new BackpressureController(options);

        var oldChunkId = ChunkId.NewChunkId();
        using (var builder = new ChunkBuilder(options))
        {
            builder.Append(Encoding.UTF8.GetBytes("\"old\""));
            var (data, metadata) = builder.Seal();
            await store.SealAsync(oldChunkId, data, metadata with { ChunkId = oldChunkId.Value }, TestContext.CancellationToken);
        }

        using var buffer = new DurableBuffer<string>(
            new JsonRecordSerializer<string>(),
            store,
            options,
            metrics,
            events,
            backpressure,
            chunkChannel.Writer);
        metrics.AddDiskBytes(options.MaxDiskBytes);

        var result = await buffer.WriteAsync("new", TestContext.CancellationToken);
        await buffer.FlushAsync(TestContext.CancellationToken);

        Assert.AreEqual(BufferWriteStatus.DroppedOldestAndAccepted, result.Status);
        Assert.IsFalse(File.Exists(Path.Combine(_storagePath, "sealed", $"{oldChunkId.Value}.chunk")));
        Assert.IsTrue(chunkChannel.Reader.TryRead(out var newChunk));
        Assert.AreNotEqual(oldChunkId, newChunk.Id);
    }

    private sealed class EventCollector(List<BufferEvent> events) : IObserver<BufferEvent>
    {
        public void OnCompleted() { }
        public void OnError(Exception error) { }
        public void OnNext(BufferEvent value)
        {
            lock (events)
            {
                events.Add(value);
            }
        }
    }

    public TestContext TestContext { get; set; }
}