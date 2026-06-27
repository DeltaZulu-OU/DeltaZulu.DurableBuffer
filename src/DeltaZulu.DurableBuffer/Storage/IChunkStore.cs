using DeltaZulu.DurableBuffer.Chunks;

namespace DeltaZulu.DurableBuffer.Storage;

public interface IChunkStore
{
    ValueTask<StoredChunk> SealAsync(
        ChunkId chunkId,
        ReadOnlyMemory<byte> chunkData,
        ChunkMetadata metadata,
        CancellationToken cancellationToken = default);

    ValueTask<StoredChunk> MoveToDispatchingAsync(
        StoredChunk chunk,
        CancellationToken cancellationToken = default);

    ValueTask<StoredChunk> MoveToSealedAsync(
        StoredChunk chunk,
        ChunkMetadata updatedMetadata,
        CancellationToken cancellationToken = default);

    ValueTask DeleteAsync(
        StoredChunk chunk,
        CancellationToken cancellationToken = default);

    ValueTask<StoredChunk> MoveToDeadLetterAsync(
        StoredChunk chunk,
        CancellationToken cancellationToken = default);

    ValueTask QuarantineAsync(
        string filePath,
        CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<StoredChunk>> GetSealedChunksAsync(
        CancellationToken cancellationToken = default);

    ValueTask<IReadOnlyList<StoredChunk>> GetDispatchingChunksAsync(
        CancellationToken cancellationToken = default);

    ValueTask<long> GetDiskBytesUsedAsync(
        CancellationToken cancellationToken = default);
}