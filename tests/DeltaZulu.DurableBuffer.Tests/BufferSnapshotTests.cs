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
            State = BufferState.Pressured,
            DiskBytesUsed = 100_000,
            DiskBytesLimit = 512L * 1024 * 1024,
            MemoryBytesUsed = 10_000,
            OpenChunkBytes = 5_000,
            SealedChunkCount = 3,
            OldestChunkAge = TimeSpan.FromSeconds(42),
            RecordsAcceptedTotal = 1000,
            RecordsRejectedTotal = 5,
            RecordsDroppedTotal = 2,
            ChunksCompletedTotal = 10,
            ChunksReleasedTotal = 1,
            ChunksDeadLetteredTotal = 1,
            DeadLetterBytesUsed = 0,
            DeadLetterBytesLimit = 64L * 1024 * 1024,
            QuarantineBytesUsed = 0,
            QuarantineBytesLimit = 64L * 1024 * 1024,
            ChunksDeadLetterEvictedTotal = 0,
            ChunksQuarantineEvictedTotal = 0,
            DispatchQueueCapacity = 1024,
            DispatchQueueDepth = 1,
            MaxInFlightChunks = 256,
            InFlightChunks = 1,
            AvailableChunks = 2,
            OldestAvailableChunkAge = TimeSpan.FromSeconds(20),
            OldestDispatchedChunkAge = TimeSpan.FromSeconds(10),
            DispatcherWaitReason = DispatchWaitReason.None
        };

        Assert.AreEqual(BufferState.Pressured, snapshot.State);
        Assert.AreEqual(100_000, snapshot.DiskBytesUsed);
        Assert.AreEqual(3, snapshot.SealedChunkCount);
        Assert.AreEqual(TimeSpan.FromSeconds(42), snapshot.OldestChunkAge);
        Assert.AreEqual(1000, snapshot.RecordsAcceptedTotal);
        Assert.AreEqual(5, snapshot.RecordsRejectedTotal);
        Assert.AreEqual(2, snapshot.RecordsDroppedTotal);
        Assert.AreEqual(10, snapshot.ChunksCompletedTotal);
        Assert.AreEqual(1, snapshot.ChunksReleasedTotal);
        Assert.AreEqual(1, snapshot.ChunksDeadLetteredTotal);
    }
}