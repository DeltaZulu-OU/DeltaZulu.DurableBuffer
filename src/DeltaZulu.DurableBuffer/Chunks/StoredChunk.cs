namespace DeltaZulu.DurableBuffer.Chunks;

public sealed record StoredChunk
{
    public required ChunkId Id { get; init; }
    public required string ChunkFilePath { get; init; }
    public required string MetadataFilePath { get; init; }
    public required ChunkMetadata Metadata { get; init; }
}
