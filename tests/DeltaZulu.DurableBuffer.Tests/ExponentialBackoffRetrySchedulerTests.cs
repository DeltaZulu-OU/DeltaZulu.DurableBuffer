using DeltaZulu.DurableBuffer.Configuration;
using DeltaZulu.DurableBuffer.Retry;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class ExponentialBackoffRetrySchedulerTests
{
    private static DurableBufferOptions DefaultOptions => new()
    {
        StoragePath = "/tmp/test",
        MaxRetryAttempts = 5,
        RetryBaseDelay = TimeSpan.FromSeconds(1),
        RetryMaxDelay = TimeSpan.FromMinutes(5)
    };

    [TestMethod]
    public void CalculateNextAttempt_DelayIncreasesWithAttempts()
    {
        var scheduler = new ExponentialBackoffRetryScheduler(DefaultOptions);
        var now = DateTimeOffset.UtcNow;

        var attempt1 = scheduler.CalculateNextAttempt(0);
        var attempt2 = scheduler.CalculateNextAttempt(1);
        var attempt3 = scheduler.CalculateNextAttempt(2);

        Assert.IsGreaterThan(now, attempt1);
        Assert.IsGreaterThan(attempt1 - TimeSpan.FromSeconds(2), attempt2);
        Assert.IsGreaterThan(attempt2 - TimeSpan.FromSeconds(4), attempt3);
    }

    [TestMethod]
    public void CalculateNextAttempt_RespectMaxDelay()
    {
        var options = DefaultOptions with { RetryMaxDelay = TimeSpan.FromSeconds(10) };
        var scheduler = new ExponentialBackoffRetryScheduler(options);

        var attempt = scheduler.CalculateNextAttempt(30);
        var maxExpected = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(15);

        Assert.IsLessThan(maxExpected, attempt);
    }

    [TestMethod]
    public void IsRetryExhausted_BelowMax_ReturnsFalse()
    {
        var scheduler = new ExponentialBackoffRetryScheduler(DefaultOptions);
        Assert.IsFalse(scheduler.IsRetryExhausted(0));
        Assert.IsFalse(scheduler.IsRetryExhausted(4));
    }

    [TestMethod]
    public void IsRetryExhausted_AtMax_ReturnsTrue()
    {
        var scheduler = new ExponentialBackoffRetryScheduler(DefaultOptions);
        Assert.IsTrue(scheduler.IsRetryExhausted(5));
        Assert.IsTrue(scheduler.IsRetryExhausted(10));
    }
}
