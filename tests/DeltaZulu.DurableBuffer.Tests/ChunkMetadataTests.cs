using System.Text.Json;
using DeltaZulu.DurableBuffer.Chunks;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class ChunkMetadataTests
{
    [TestMethod]
    public void RoundTrips_ThroughJson()
    {
        var metadata = new ChunkMetadata
        {
            ChunkId = "abc123",
            CreatedUtc = new DateTimeOffset(2026, 6, 25, 17, 1, 2, TimeSpan.Zero),
            SealedUtc = new DateTimeOffset(2026, 6, 25, 17, 1, 7, TimeSpan.Zero),
            RecordCount = 500,
            PayloadBytes = 1048576,
            Checksum = "sha256:deadbeef",
            AttemptCount = 0,
            Source = "DeltaZulu.Agent"
        };

        var json = JsonSerializer.Serialize(metadata);
        var deserialized = JsonSerializer.Deserialize<ChunkMetadata>(json);

        Assert.IsNotNull(deserialized);
        Assert.AreEqual(metadata.ChunkId, deserialized.ChunkId);
        Assert.AreEqual(metadata.CreatedUtc, deserialized.CreatedUtc);
        Assert.AreEqual(metadata.SealedUtc, deserialized.SealedUtc);
        Assert.AreEqual(metadata.RecordCount, deserialized.RecordCount);
        Assert.AreEqual(metadata.PayloadBytes, deserialized.PayloadBytes);
        Assert.AreEqual(metadata.Checksum, deserialized.Checksum);
        Assert.AreEqual(metadata.AttemptCount, deserialized.AttemptCount);
        Assert.IsNull(deserialized.NextAttemptUtc);
        Assert.IsNull(deserialized.LastError);
        Assert.AreEqual("DeltaZulu.Agent", deserialized.Source);
    }

    [TestMethod]
    public void RetryFields_RoundTrip()
    {
        var metadata = new ChunkMetadata
        {
            ChunkId = "retry-test",
            CreatedUtc = DateTimeOffset.UtcNow,
            RecordCount = 10,
            PayloadBytes = 1024,
            Checksum = "sha256:abc",
            AttemptCount = 3,
            NextAttemptUtc = DateTimeOffset.UtcNow.AddSeconds(30),
            LastError = "Connection refused"
        };

        var json = JsonSerializer.Serialize(metadata);
        var deserialized = JsonSerializer.Deserialize<ChunkMetadata>(json);

        Assert.IsNotNull(deserialized);
        Assert.AreEqual(3, deserialized.AttemptCount);
        Assert.IsNotNull(deserialized.NextAttemptUtc);
        Assert.AreEqual("Connection refused", deserialized.LastError);
    }
}
