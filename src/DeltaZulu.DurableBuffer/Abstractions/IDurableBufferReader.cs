using System.Threading.Channels;
using DeltaZulu.DurableBuffer.Chunks;

namespace DeltaZulu.DurableBuffer.Abstractions;

public interface IDurableBufferReader
{
    ChannelReader<StoredChunk> SealedChunks { get; }

    ValueTask CompleteAsync(
        StoredChunk chunk,
        CancellationToken cancellationToken = default);

    ValueTask DeadLetterAsync(
        StoredChunk chunk,
        string? reason = null,
        CancellationToken cancellationToken = default);

    ValueTask ReleaseAsync(
            StoredChunk chunk,
        CancellationToken cancellationToken = default);
}
