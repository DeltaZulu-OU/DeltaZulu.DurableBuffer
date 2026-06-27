namespace DeltaZulu.DurableBuffer.Configuration;

public sealed record DurableBufferOptions
{
    public required string StoragePath { get; init; }

    public long MaxDiskBytes { get; init; } = 512L * 1024 * 1024;
    public long MaxMemoryBytes { get; init; } = 32L * 1024 * 1024;

    public int MaxChunkRecords { get; init; } = 1000;
    public long MaxChunkBytes { get; init; } = 4L * 1024 * 1024;
    public TimeSpan MaxChunkAge { get; init; } = TimeSpan.FromSeconds(5);

    public BufferFullPolicy FullPolicy { get; init; } = BufferFullPolicy.Block;
    public RetryExhaustedPolicy RetryExhaustedPolicy { get; init; } = RetryExhaustedPolicy.DeadLetter;

    public int MaxRetryAttempts { get; init; } = 10;
    public TimeSpan RetryBaseDelay { get; init; } = TimeSpan.FromSeconds(1);
    public TimeSpan RetryMaxDelay { get; init; } = TimeSpan.FromMinutes(5);
}