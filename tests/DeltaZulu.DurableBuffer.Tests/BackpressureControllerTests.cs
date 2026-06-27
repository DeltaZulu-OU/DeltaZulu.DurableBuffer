using DeltaZulu.DurableBuffer.Abstractions;
using DeltaZulu.DurableBuffer.Configuration;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class BackpressureControllerTests
{
    private static DurableBufferOptions DefaultOptions => new()
    {
        StoragePath = "/tmp/test",
        MaxDiskBytes = 1000,
        MaxMemoryBytes = 100
    };

    [TestMethod]
    public void Evaluate_Healthy_WhenUnderLimits()
    {
        var controller = new BackpressureController(DefaultOptions);

        var (state, accept) = controller.Evaluate(diskBytesUsed: 100, memoryBytesUsed: 10, retryQueueDepth: 0);

        Assert.AreEqual(BufferState.Healthy, state);
        Assert.IsTrue(accept);
    }

    [TestMethod]
    public void Evaluate_Degraded_WhenRetryQueueNotEmpty()
    {
        var controller = new BackpressureController(DefaultOptions);

        var (state, accept) = controller.Evaluate(diskBytesUsed: 100, memoryBytesUsed: 10, retryQueueDepth: 1);

        Assert.AreEqual(BufferState.Degraded, state);
        Assert.IsTrue(accept);
    }

    [TestMethod]
    public void Evaluate_Pressured_WhenDiskAbove85Percent()
    {
        var controller = new BackpressureController(DefaultOptions);

        var (state, accept) = controller.Evaluate(diskBytesUsed: 860, memoryBytesUsed: 10, retryQueueDepth: 0);

        Assert.AreEqual(BufferState.Pressured, state);
        Assert.IsTrue(accept);
    }

    [TestMethod]
    public void Evaluate_Pressured_WhenMemoryAbove85Percent()
    {
        var controller = new BackpressureController(DefaultOptions);

        var (state, accept) = controller.Evaluate(diskBytesUsed: 100, memoryBytesUsed: 86, retryQueueDepth: 0);

        Assert.AreEqual(BufferState.Pressured, state);
        Assert.IsTrue(accept);
    }

    [TestMethod]
    public void Evaluate_Full_WhenDiskAtLimit()
    {
        var controller = new BackpressureController(DefaultOptions);

        var (state, accept) = controller.Evaluate(diskBytesUsed: 1000, memoryBytesUsed: 10, retryQueueDepth: 0);

        Assert.AreEqual(BufferState.Full, state);
        Assert.IsFalse(accept);
    }

    [TestMethod]
    public void Evaluate_Full_WhenMemoryAtLimit()
    {
        var controller = new BackpressureController(DefaultOptions);

        var (state, accept) = controller.Evaluate(diskBytesUsed: 100, memoryBytesUsed: 100, retryQueueDepth: 0);

        Assert.AreEqual(BufferState.Full, state);
        Assert.IsFalse(accept);
    }

    [TestMethod]
    public void Evaluate_Full_WhenDiskExceedsLimit()
    {
        var controller = new BackpressureController(DefaultOptions);

        var (state, accept) = controller.Evaluate(diskBytesUsed: 1500, memoryBytesUsed: 10, retryQueueDepth: 0);

        Assert.AreEqual(BufferState.Full, state);
        Assert.IsFalse(accept);
    }

    [TestMethod]
    public void Evaluate_Pressured_TakesPrecedenceOverDegraded()
    {
        var controller = new BackpressureController(DefaultOptions);

        var (state, accept) = controller.Evaluate(diskBytesUsed: 860, memoryBytesUsed: 10, retryQueueDepth: 5);

        Assert.AreEqual(BufferState.Pressured, state);
        Assert.IsTrue(accept);
    }

    [TestMethod]
    public void Evaluate_Full_TakesPrecedenceOverAll()
    {
        var controller = new BackpressureController(DefaultOptions);

        var (state, accept) = controller.Evaluate(diskBytesUsed: 1000, memoryBytesUsed: 100, retryQueueDepth: 10);

        Assert.AreEqual(BufferState.Full, state);
        Assert.IsFalse(accept);
    }

    [TestMethod]
    public void Evaluate_Healthy_AtExactly85Percent()
    {
        var controller = new BackpressureController(DefaultOptions);

        var (state, _) = controller.Evaluate(diskBytesUsed: 850, memoryBytesUsed: 85, retryQueueDepth: 0);

        Assert.AreEqual(BufferState.Healthy, state);
    }
}
