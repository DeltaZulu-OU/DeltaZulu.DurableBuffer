using System.Text.Json.Serialization;

namespace DeltaZulu.DurableBuffer.Chunks;

public sealed record ChunkMetadata
{
    [JsonPropertyName("chunkId")]
    public required string ChunkId { get; init; }

    [JsonPropertyName("createdUtc")]
    public required DateTimeOffset CreatedUtc { get; init; }

    [JsonPropertyName("sealedUtc")]
    public DateTimeOffset? SealedUtc { get; init; }

    [JsonPropertyName("recordCount")]
    public required int RecordCount { get; init; }

    [JsonPropertyName("payloadBytes")]
    public required long PayloadBytes { get; init; }

    [JsonPropertyName("checksum")]
    public required string Checksum { get; init; }

    [JsonPropertyName("source")]
    public string Source { get; init; } = "DeltaZulu.Agent";
}
