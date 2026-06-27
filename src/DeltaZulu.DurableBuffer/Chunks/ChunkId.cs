namespace DeltaZulu.DurableBuffer.Chunks;

public readonly record struct ChunkId(string Value)
{
    public static ChunkId NewChunkId() => new(Guid.NewGuid().ToString("N"));

    public override string ToString() => Value;
}