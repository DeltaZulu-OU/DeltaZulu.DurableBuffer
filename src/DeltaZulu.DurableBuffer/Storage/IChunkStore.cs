using DeltaZulu.DurableBuffer.Chunks;

namespace DeltaZulu.DurableBuffer.Storage;

public interface IChunkStore
{
    ValueTask DeleteAsync(
        StoredChunk chunk,
        CancellationToken cancellationToken = default);

    ValueTask<long> GetDeadLetterBytesUsedAsync(
        CancellationToken cancellationToken = default);

    ValueTask<long> GetDiskBytesUsedAsync(
        CancellationToken cancellationToken = default);

    ValueTask<long> GetQuarantineBytesUsedAsync(
        CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<StoredChunk>> GetSealedChunksAsync(
        CancellationToken cancellationToken = default);

    ValueTask<StoredChunk> MoveToDeadLetterAsync(
        StoredChunk chunk,
        CancellationToken cancellationToken = default);

    ValueTask QuarantineAsync(
        string filePath,
        CancellationToken cancellationToken = default);

    ValueTask<StoredChunk> SealAsync(
                                    ChunkId chunkId,
        ReadOnlyMemory<byte> chunkData,
        ChunkMetadata metadata,
        CancellationToken cancellationToken = default);
}
