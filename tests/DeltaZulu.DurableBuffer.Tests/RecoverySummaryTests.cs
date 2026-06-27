using DeltaZulu.DurableBuffer.Recovery;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class RecoverySummaryTests
{
    [TestMethod]
    public void Summary_ReflectsAllProperties()
    {
        var summary = new RecoverySummary
        {
            RecoveredChunks = 5,
            QuarantinedFiles = 2,
            DeadLetteredChunks = 1,
            EstimatedLostRecords = 50
        };

        Assert.AreEqual(5, summary.RecoveredChunks);
        Assert.AreEqual(2, summary.QuarantinedFiles);
        Assert.AreEqual(1, summary.DeadLetteredChunks);
        Assert.AreEqual(50, summary.EstimatedLostRecords);
    }
}
