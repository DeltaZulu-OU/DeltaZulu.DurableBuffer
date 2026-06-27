using DeltaZulu.DurableBuffer.Abstractions;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class BufferSnapshotTests
{
    [TestMethod]
    public void Snapshot_ReflectsAllProperties()
    {
        var snapshot = new BufferSnapshot
        {
            State = BufferState.Degraded,
            DiskBytesUsed = 100_000,
            DiskBytesLimit = 512L * 1024 * 1024,
            MemoryBytesUsed = 10_000,
            OpenChunkBytes = 5_000,
            SealedChunkCount = 3,
            RetryQueueDepth = 1,
            OldestChunkAge = TimeSpan.FromSeconds(42),
            RecordsAcceptedTotal = 1000,
            RecordsRejectedTotal = 5,
            RecordsDroppedTotal = 2,
            ChunksSentTotal = 12,
            ChunksDeliveredTotal = 10,
            ChunksFailedTotal = 2,
            ChunksRetryScheduledTotal = 1,
            ChunksDeadLetteredTotal = 1
        };

        Assert.AreEqual(BufferState.Degraded, snapshot.State);
        Assert.AreEqual(100_000, snapshot.DiskBytesUsed);
        Assert.AreEqual(3, snapshot.SealedChunkCount);
        Assert.AreEqual(1, snapshot.RetryQueueDepth);
        Assert.AreEqual(TimeSpan.FromSeconds(42), snapshot.OldestChunkAge);
        Assert.AreEqual(1000, snapshot.RecordsAcceptedTotal);
        Assert.AreEqual(5, snapshot.RecordsRejectedTotal);
        Assert.AreEqual(2, snapshot.RecordsDroppedTotal);
        Assert.AreEqual(12, snapshot.ChunksSentTotal);
        Assert.AreEqual(10, snapshot.ChunksDeliveredTotal);
        Assert.AreEqual(2, snapshot.ChunksFailedTotal);
        Assert.AreEqual(1, snapshot.ChunksRetryScheduledTotal);
        Assert.AreEqual(1, snapshot.ChunksDeadLetteredTotal);
    }
}
