using DeltaZulu.DurableBuffer.Abstractions;
using DeltaZulu.DurableBuffer.Metrics;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class BufferMetricsCounterTests
{
    [TestMethod]
    public void RecordAccepted_IncrementsTotal()
    {
        var metrics = new BufferMetricsCounter();
        metrics.RecordAccepted();
        metrics.RecordAccepted();
        metrics.RecordAccepted();

        var snapshot = metrics.ToSnapshot();
        Assert.AreEqual(3, snapshot.RecordsAcceptedTotal);
    }

    [TestMethod]
    public void RecordRejected_IncrementsTotal()
    {
        var metrics = new BufferMetricsCounter();
        metrics.RecordRejected();
        metrics.RecordRejected();

        var snapshot = metrics.ToSnapshot();
        Assert.AreEqual(2, snapshot.RecordsRejectedTotal);
    }

    [TestMethod]
    public void RecordDropped_IncrementsTotal()
    {
        var metrics = new BufferMetricsCounter();
        metrics.RecordDropped();

        var snapshot = metrics.ToSnapshot();
        Assert.AreEqual(1, snapshot.RecordsDroppedTotal);
    }

    [TestMethod]
    public void ChunkCounters_IncrementCorrectly()
    {
        var metrics = new BufferMetricsCounter();
        metrics.ChunkCreated();
        metrics.ChunkSealed();
        metrics.ChunkCompleted();
        metrics.ChunkCompleted();
        metrics.ChunkReleased();
        metrics.ChunkDeadLettered();
        metrics.ChunkQuarantined();

        var snapshot = metrics.ToSnapshot();
        Assert.AreEqual(2, snapshot.ChunksCompletedTotal);
        Assert.AreEqual(1, snapshot.ChunksReleasedTotal);
        Assert.AreEqual(1, snapshot.ChunksDeadLetteredTotal);
    }

    [TestMethod]
    public void UpdateState_ReflectedInSnapshot()
    {
        var metrics = new BufferMetricsCounter();
        metrics.UpdateState(BufferState.Pressured);

        var snapshot = metrics.ToSnapshot();
        Assert.AreEqual(BufferState.Pressured, snapshot.State);
    }

    [TestMethod]
    public void UpdateDiskUsage_ReflectedInSnapshot()
    {
        var metrics = new BufferMetricsCounter();
        metrics.UpdateDiskUsage(500, 1000);

        var snapshot = metrics.ToSnapshot();
        Assert.AreEqual(500, snapshot.DiskBytesUsed);
        Assert.AreEqual(1000, snapshot.DiskBytesLimit);
    }

    [TestMethod]
    public void UpdateMemoryUsage_ReflectedInSnapshot()
    {
        var metrics = new BufferMetricsCounter();
        metrics.UpdateMemoryUsage(256);

        var snapshot = metrics.ToSnapshot();
        Assert.AreEqual(256, snapshot.MemoryBytesUsed);
    }

    [TestMethod]
    public void UpdateOpenChunkBytes_ReflectedInSnapshot()
    {
        var metrics = new BufferMetricsCounter();
        metrics.UpdateOpenChunkBytes(1024);

        var snapshot = metrics.ToSnapshot();
        Assert.AreEqual(1024, snapshot.OpenChunkBytes);
    }

    [TestMethod]
    public void UpdateSealedChunkCount_ReflectedInSnapshot()
    {
        var metrics = new BufferMetricsCounter();
        metrics.UpdateSealedChunkCount(5);

        var snapshot = metrics.ToSnapshot();
        Assert.AreEqual(5, snapshot.SealedChunkCount);
    }

    [TestMethod]
    public void UpdateOldestChunkAge_WithValue_ReflectedInSnapshot()
    {
        var metrics = new BufferMetricsCounter();
        var age = TimeSpan.FromMinutes(5);
        metrics.UpdateOldestChunkAge(age);

        var snapshot = metrics.ToSnapshot();
        Assert.IsNotNull(snapshot.OldestChunkAge);
        Assert.AreEqual(age, snapshot.OldestChunkAge);
    }

    [TestMethod]
    public void UpdateOldestChunkAge_Null_ReflectedInSnapshot()
    {
        var metrics = new BufferMetricsCounter();
        metrics.UpdateOldestChunkAge(TimeSpan.FromMinutes(5));
        metrics.UpdateOldestChunkAge(null);

        var snapshot = metrics.ToSnapshot();
        Assert.IsNull(snapshot.OldestChunkAge);
    }

    [TestMethod]
    public void DiskBytesUsed_ReturnsCurrentValue()
    {
        var metrics = new BufferMetricsCounter();
        metrics.UpdateDiskUsage(42, 1000);

        Assert.AreEqual(42, metrics.DiskBytesUsed);
    }

    [TestMethod]
    public void AddDiskBytes_AdjustsTotal()
    {
        var metrics = new BufferMetricsCounter();
        metrics.UpdateDiskUsage(100, 1000);
        metrics.AddDiskBytes(50);

        Assert.AreEqual(150, metrics.DiskBytesUsed);
    }

    [TestMethod]
    public void AddDiskBytes_SupportsNegativeValues()
    {
        var metrics = new BufferMetricsCounter();
        metrics.UpdateDiskUsage(100, 1000);
        metrics.AddDiskBytes(-30);

        Assert.AreEqual(70, metrics.DiskBytesUsed);
    }

    [TestMethod]
    public void ToSnapshot_DefaultState_IsHealthy()
    {
        var metrics = new BufferMetricsCounter();
        var snapshot = metrics.ToSnapshot();

        Assert.AreEqual(BufferState.Healthy, snapshot.State);
        Assert.AreEqual(0, snapshot.DiskBytesUsed);
        Assert.AreEqual(0, snapshot.MemoryBytesUsed);
        Assert.AreEqual(0, snapshot.RecordsAcceptedTotal);
        Assert.IsNull(snapshot.OldestChunkAge);
    }
}