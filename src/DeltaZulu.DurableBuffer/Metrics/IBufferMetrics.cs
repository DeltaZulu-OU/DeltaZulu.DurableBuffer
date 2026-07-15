using DeltaZulu.DurableBuffer.Abstractions;

namespace DeltaZulu.DurableBuffer.Metrics;

public interface IBufferMetrics
{
    void ChunkCompleted();

    void ChunkCreated();

    void ChunkDeadLettered();

    void ChunkDeadLetterEvicted();

    void ChunkQuarantined();

    void ChunkQuarantineEvicted();

    void ChunkReleased();

    void ChunkSealed();

    void RecordAccepted();

    void RecordDropped();

    void RecordRejected();

    BufferSnapshot ToSnapshot();

    void UpdateDeadLetterUsage(long bytesUsed, long bytesLimit);

    void UpdateDiskUsage(long bytesUsed, long bytesLimit);

    void UpdateMemoryUsage(long bytesUsed);

    void UpdateOldestChunkAge(TimeSpan? age);

    void UpdateOpenChunkBytes(long bytes);

    void UpdateQuarantineUsage(long bytesUsed, long bytesLimit);

    void UpdateSealedChunkCount(int count);

    void UpdateState(BufferState state);
}
