using DeltaZulu.DurableBuffer.Chunks;
using DeltaZulu.DurableBuffer.Metrics;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class BufferEventTests
{
    [TestMethod]
    public void Create_WithoutChunk_HasNullChunk()
    {
        var evt = BufferEvent.Create(BufferEventType.BufferStarted);

        Assert.AreEqual(BufferEventType.BufferStarted, evt.EventType);
        Assert.IsNull(evt.ChunkId);
        Assert.IsNull(evt.Detail);
        Assert.IsNull(evt.Chunk);
    }

    [TestMethod]
    public void Create_WithChunkId_PreservesId()
    {
        var evt = BufferEvent.Create(BufferEventType.BufferChunkSealed, chunkId: "abc123");

        Assert.AreEqual("abc123", evt.ChunkId);
        Assert.IsNull(evt.Chunk);
    }

    [TestMethod]
    public void Create_WithStoredChunk_PreservesReference()
    {
        var metadata = new ChunkMetadata
        {
            ChunkId = "dead-letter-1",
            CreatedUtc = DateTimeOffset.UtcNow,
            RecordCount = 50,
            PayloadBytes = 4096,
            Checksum = "sha256:abc",
            AttemptCount = 10,
            LastError = "Connection refused"
        };

        var chunk = new StoredChunk
        {
            Id = new ChunkId("dead-letter-1"),
            ChunkFilePath = "/buffer/deadletter/chunk.bin",
            MetadataFilePath = "/buffer/deadletter/chunk.meta.json",
            Metadata = metadata
        };

        var evt = BufferEvent.Create(
            BufferEventType.BufferChunkDeadLettered,
            chunkId: "dead-letter-1",
            detail: "Retry exhausted after 10 attempts",
            chunk: chunk);

        Assert.AreEqual(BufferEventType.BufferChunkDeadLettered, evt.EventType);
        Assert.AreEqual("dead-letter-1", evt.ChunkId);
        Assert.IsNotNull(evt.Chunk);
        Assert.AreEqual("/buffer/deadletter/chunk.bin", evt.Chunk.ChunkFilePath);
        Assert.AreEqual(50, evt.Chunk.Metadata.RecordCount);
        Assert.AreEqual(10, evt.Chunk.Metadata.AttemptCount);
    }

    [TestMethod]
    public void Create_TimestampUtc_IsPopulated()
    {
        var before = DateTimeOffset.UtcNow;
        var evt = BufferEvent.Create(BufferEventType.BufferRecoveryCompleted);
        var after = DateTimeOffset.UtcNow;

        Assert.IsGreaterThanOrEqualTo(before, evt.TimestampUtc);
        Assert.IsLessThanOrEqualTo(after, evt.TimestampUtc);
    }
}
