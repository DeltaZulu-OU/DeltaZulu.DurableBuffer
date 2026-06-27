using DeltaZulu.DurableBuffer.Configuration;

namespace DeltaZulu.DurableBuffer.Retry;

public sealed class ExponentialBackoffRetryScheduler : IRetryScheduler
{
    private readonly int _maxAttempts;
    private readonly double _baseDelayMs;
    private readonly double _maxDelayMs;

    public ExponentialBackoffRetryScheduler(DurableBufferOptions options)
    {
        _maxAttempts = options.MaxRetryAttempts;
        _baseDelayMs = options.RetryBaseDelay.TotalMilliseconds;
        _maxDelayMs = options.RetryMaxDelay.TotalMilliseconds;
    }

    public DateTimeOffset CalculateNextAttempt(int attemptCount)
    {
        var delayMs = Math.Min(_maxDelayMs, _baseDelayMs * (1L << Math.Min(attemptCount, 30)));
        var jitter = delayMs * ((Random.Shared.NextDouble() * 0.5) - 0.25);
        return DateTimeOffset.UtcNow + TimeSpan.FromMilliseconds(delayMs + jitter);
    }

    public bool IsRetryExhausted(int attemptCount) => attemptCount >= _maxAttempts;
}