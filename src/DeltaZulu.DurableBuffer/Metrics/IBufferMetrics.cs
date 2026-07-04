using DeltaZulu.DurableBuffer.Abstractions;

namespace DeltaZulu.DurableBuffer.Metrics;

public interface IBufferMetrics
{
    void RecordAccepted();

    void RecordRejected();

    void RecordDropped();

    void ChunkCreated();

    void ChunkSealed();

    void ChunkCompleted();

    void ChunkReleased();

    void ChunkDeadLettered();

    void ChunkQuarantined();

    void ChunkDeadLetterEvicted();

    void ChunkQuarantineEvicted();

    void UpdateState(BufferState state);

    void UpdateDiskUsage(long bytesUsed, long bytesLimit);

    void UpdateDeadLetterUsage(long bytesUsed, long bytesLimit);

    void UpdateQuarantineUsage(long bytesUsed, long bytesLimit);

    void UpdateMemoryUsage(long bytesUsed);

    void UpdateOpenChunkBytes(long bytes);

    void UpdateSealedChunkCount(int count);

    void UpdateOldestChunkAge(TimeSpan? age);

    BufferSnapshot ToSnapshot();
}