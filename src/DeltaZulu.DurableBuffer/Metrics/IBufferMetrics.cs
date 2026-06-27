using DeltaZulu.DurableBuffer.Abstractions;

namespace DeltaZulu.DurableBuffer.Metrics;

public interface IBufferMetrics
{
    void RecordAccepted();

    void RecordRejected();

    void RecordDropped();

    void ChunkCreated();

    void ChunkSealed();

    void ChunkSent();

    void ChunkDelivered();

    void ChunkFailed();

    void ChunkRetried();

    void ChunkDeadLettered();

    void ChunkQuarantined();

    void UpdateState(BufferState state);

    void UpdateDiskUsage(long bytesUsed, long bytesLimit);

    void UpdateMemoryUsage(long bytesUsed);

    void UpdateOpenChunkBytes(long bytes);

    void UpdateSealedChunkCount(int count);

    void UpdateOldestChunkAge(TimeSpan? age);

    void UpdateRetryQueueDepth(int depth);

    BufferSnapshot ToSnapshot();
}