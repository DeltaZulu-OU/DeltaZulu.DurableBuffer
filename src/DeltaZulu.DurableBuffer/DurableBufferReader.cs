using System.Threading.Channels;
using DeltaZulu.DurableBuffer.Abstractions;
using DeltaZulu.DurableBuffer.Chunks;
using DeltaZulu.DurableBuffer.Metrics;
using DeltaZulu.DurableBuffer.Storage;

namespace DeltaZulu.DurableBuffer;

internal sealed class DurableBufferReader : IDurableBufferReader
{
    private readonly ChannelReader<StoredChunk> _channelReader;
    private readonly ChannelWriter<StoredChunk> _channelWriter;
    private readonly IChunkStore _store;
    private readonly BufferMetricsCounter _metrics;
    private readonly BufferEventBroadcaster _events;
    private readonly Action? _signalSpaceAvailable;

    public DurableBufferReader(
        ChannelReader<StoredChunk> channelReader,
        ChannelWriter<StoredChunk> channelWriter,
        IChunkStore store,
        BufferMetricsCounter metrics,
        BufferEventBroadcaster events,
        Action? signalSpaceAvailable = null)
    {
        _channelReader = channelReader;
        _channelWriter = channelWriter;
        _store = store;
        _metrics = metrics;
        _events = events;
        _signalSpaceAvailable = signalSpaceAvailable;
    }

    public ChannelReader<StoredChunk> SealedChunks => _channelReader;

    public async ValueTask CompleteAsync(
        StoredChunk chunk,
        CancellationToken cancellationToken = default)
    {
        await _store.DeleteAsync(chunk, cancellationToken);
        _metrics.ChunkCompleted();
        _metrics.AddDiskBytes(-chunk.Metadata.PayloadBytes);
        _signalSpaceAvailable?.Invoke();
        _events.Publish(BufferEvent.Create(
            BufferEventType.BufferChunkCompleted, chunk.Id.Value, chunk: chunk));
    }

    public async ValueTask ReleaseAsync(
        StoredChunk chunk,
        CancellationToken cancellationToken = default)
    {
        await _channelWriter.WriteAsync(chunk, cancellationToken);
        _metrics.ChunkReleased();
        _events.Publish(BufferEvent.Create(
            BufferEventType.BufferChunkReleased, chunk.Id.Value,
            detail: "Released for retry", chunk: chunk));
    }

    public async ValueTask DeadLetterAsync(
        StoredChunk chunk,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        var deadLettered = await _store.MoveToDeadLetterAsync(chunk, cancellationToken);
        _metrics.AddDiskBytes(-chunk.Metadata.PayloadBytes);
        _metrics.AddDeadLetterBytes(chunk.Metadata.PayloadBytes);
        _signalSpaceAvailable?.Invoke();
        _metrics.ChunkDeadLettered();
        _events.Publish(BufferEvent.Create(
            BufferEventType.BufferChunkDeadLettered, deadLettered.Id.Value,
            detail: reason, chunk: deadLettered));
    }
}