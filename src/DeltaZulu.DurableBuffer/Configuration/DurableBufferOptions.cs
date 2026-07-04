namespace DeltaZulu.DurableBuffer.Configuration;

public sealed record DurableBufferOptions
{
    public required string StoragePath { get; init; }

    public long MaxDiskBytes { get; init; } = 512L * 1024 * 1024;
    public long MaxMemoryBytes { get; init; } = 32L * 1024 * 1024;

    /// <summary>
    /// Dead-letter storage is a bounded ring buffer: once this cap is reached, the oldest
    /// dead-lettered chunks (by original record age) are evicted to make room for new ones.
    /// It is independent of <see cref="MaxDiskBytes"/> so abandoned data never blocks fresh writes.
    /// </summary>
    public long MaxDeadLetterBytes { get; init; } = 64L * 1024 * 1024;

    /// <summary>
    /// Quarantine storage is a bounded ring buffer: once this cap is reached, the oldest
    /// quarantined files (by quarantine time) are evicted to make room for new ones.
    /// It is independent of <see cref="MaxDiskBytes"/> so corrupt data never blocks fresh writes.
    /// </summary>
    public long MaxQuarantineBytes { get; init; } = 64L * 1024 * 1024;
    public int MaxChunkRecords { get; init; } = 1000;
    public long MaxChunkBytes { get; init; } = 4L * 1024 * 1024;
    public TimeSpan MaxChunkAge { get; init; } = TimeSpan.FromSeconds(5);

    public BufferFullPolicy FullPolicy { get; init; } = BufferFullPolicy.Block;
}