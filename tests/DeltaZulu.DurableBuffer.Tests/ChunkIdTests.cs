using DeltaZulu.DurableBuffer.Chunks;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class ChunkIdTests
{
    [TestMethod]
    public void NewChunkId_GeneratesUniqueValues()
    {
        var id1 = ChunkId.NewChunkId();
        var id2 = ChunkId.NewChunkId();

        Assert.AreNotEqual(id1, id2);
    }

    [TestMethod]
    public void ToString_ReturnsValue()
    {
        var id = new ChunkId("test-id");
        Assert.AreEqual("test-id", id.ToString());
    }

    [TestMethod]
    public void Equality_WorksCorrectly()
    {
        var id1 = new ChunkId("same");
        var id2 = new ChunkId("same");
        var id3 = new ChunkId("different");

        Assert.AreEqual(id1, id2);
        Assert.AreNotEqual(id1, id3);
    }
}
