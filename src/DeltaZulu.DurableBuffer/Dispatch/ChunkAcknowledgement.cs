using System.Text.Json.Serialization;

namespace DeltaZulu.DurableBuffer.Dispatch;

public sealed record ChunkAcknowledgement
{
    [JsonPropertyName("chunkId")]
    public required string ChunkId { get; init; }

    [JsonPropertyName("accepted")]
    public required bool Accepted { get; init; }

    [JsonPropertyName("recordCount")]
    public int? RecordCount { get; init; }

    [JsonPropertyName("checksum")]
    public string? Checksum { get; init; }
}