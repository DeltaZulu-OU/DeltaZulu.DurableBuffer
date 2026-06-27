using DeltaZulu.DurableBuffer.Abstractions;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class BufferWriteResultTests
{
    [TestMethod]
    public void Accepted_IsAccepted_ReturnsTrue()
    {
        var result = new BufferWriteResult(BufferWriteStatus.Accepted);
        Assert.IsTrue(result.IsAccepted);
    }

    [TestMethod]
    public void DroppedOldestAndAccepted_IsAccepted_ReturnsTrue()
    {
        var result = new BufferWriteResult(BufferWriteStatus.DroppedOldestAndAccepted);
        Assert.IsTrue(result.IsAccepted);
    }

    [TestMethod]
    [DataRow(BufferWriteStatus.RejectedBufferFull)]
    [DataRow(BufferWriteStatus.RejectedRecordTooLarge)]
    [DataRow(BufferWriteStatus.RejectedStopping)]
    public void Rejected_IsAccepted_ReturnsFalse(BufferWriteStatus status)
    {
        var result = new BufferWriteResult(status);
        Assert.IsFalse(result.IsAccepted);
    }
}
