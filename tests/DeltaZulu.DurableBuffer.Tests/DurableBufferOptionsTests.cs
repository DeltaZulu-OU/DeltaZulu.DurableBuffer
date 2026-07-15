using DeltaZulu.DurableBuffer.Configuration;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class DurableBufferOptionsTests
{
    [TestMethod]
    public void Defaults_AreCorrect()
    {
        var options = new DurableBufferOptions { StoragePath = "/tmp/buffer" };

        Assert.AreEqual(512L * 1024 * 1024, options.MaxDiskBytes);
        Assert.AreEqual(32L * 1024 * 1024, options.MaxMemoryBytes);
        Assert.AreEqual(64L * 1024 * 1024, options.MaxDeadLetterBytes);
        Assert.AreEqual(64L * 1024 * 1024, options.MaxQuarantineBytes);
        Assert.AreEqual(1000, options.MaxChunkRecords);
        Assert.AreEqual(4L * 1024 * 1024, options.MaxChunkBytes);
        Assert.AreEqual(TimeSpan.FromSeconds(5), options.MaxChunkAge);
        Assert.AreEqual(1024, options.DispatchChannelCapacity);
        Assert.AreEqual(256, options.MaxInFlightChunks);
        Assert.AreEqual(BufferFullPolicy.Block, options.FullPolicy);
    }

    [TestMethod]
    public void CustomValues_ArePreserved()
    {
        var options = new DurableBufferOptions {
            StoragePath = "/data/buffer",
            MaxDiskBytes = 1024L * 1024 * 1024,
            MaxChunkRecords = 500,
            DispatchChannelCapacity = 32,
            MaxInFlightChunks = 8,
            FullPolicy = BufferFullPolicy.RejectNewest
        };

        Assert.AreEqual("/data/buffer", options.StoragePath);
        Assert.AreEqual(1024L * 1024 * 1024, options.MaxDiskBytes);
        Assert.AreEqual(500, options.MaxChunkRecords);
        Assert.AreEqual(32, options.DispatchChannelCapacity);
        Assert.AreEqual(8, options.MaxInFlightChunks);
        Assert.AreEqual(BufferFullPolicy.RejectNewest, options.FullPolicy);
    }
}