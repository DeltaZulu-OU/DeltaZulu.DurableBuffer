using System.Text.Json;
using DeltaZulu.DurableBuffer.Chunks;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class ChunkMetadataTests
{
    [TestMethod]
    public void RoundTrips_ThroughJson()
    {
        var metadata = new ChunkMetadata {
            ChunkId = "abc123",
            CreatedUtc = new DateTimeOffset(2026, 6, 25, 17, 1, 2, TimeSpan.Zero),
            SealedUtc = new DateTimeOffset(2026, 6, 25, 17, 1, 7, TimeSpan.Zero),
            RecordCount = 500,
            PayloadBytes = 1048576,
            Checksum = "sha256:deadbeef",
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
        Assert.AreEqual("DeltaZulu.Agent", deserialized.Source);
    }

    [TestMethod]
    public void Deserialize_IgnoresLegacyRetryFields()
    {
        const string legacyJson = """
            {
                "chunkId": "legacy-1",
                "createdUtc": "2026-06-25T17:01:02+00:00",
                "recordCount": 10,
                "payloadBytes": 1024,
                "checksum": "sha256:abc",
                "attemptCount": 3,
                "nextAttemptUtc": "2026-06-25T17:02:02+00:00",
                "lastError": "Connection refused"
            }
            """;

        var deserialized = JsonSerializer.Deserialize<ChunkMetadata>(legacyJson);

        Assert.IsNotNull(deserialized);
        Assert.AreEqual("legacy-1", deserialized.ChunkId);
        Assert.AreEqual(10, deserialized.RecordCount);
    }
}