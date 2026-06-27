using DeltaZulu.DurableBuffer.Dispatch;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class ChunkSendResultTests
{
    [TestMethod]
    public void Success_HasNoError()
    {
        var result = new ChunkSendResult(ChunkSendStatus.Success);
        Assert.AreEqual(ChunkSendStatus.Success, result.Status);
        Assert.IsNull(result.Error);
    }

    [TestMethod]
    public void TransientFailure_CarriesError()
    {
        var result = new ChunkSendResult(ChunkSendStatus.TransientFailure, "Connection refused");
        Assert.AreEqual(ChunkSendStatus.TransientFailure, result.Status);
        Assert.AreEqual("Connection refused", result.Error);
    }

    [TestMethod]
    public void PermanentFailure_CarriesError()
    {
        var result = new ChunkSendResult(ChunkSendStatus.PermanentFailure, "Invalid payload");
        Assert.AreEqual(ChunkSendStatus.PermanentFailure, result.Status);
        Assert.AreEqual("Invalid payload", result.Error);
    }
}
