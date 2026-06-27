using DeltaZulu.DurableBuffer.Chunks;

namespace DeltaZulu.DurableBuffer.Dispatch;

public interface IChunkSender
{
    ValueTask<ChunkSendResult> SendAsync(
        StoredChunk chunk,
        CancellationToken cancellationToken = default);
}