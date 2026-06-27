using System.Text;
using DeltaZulu.DurableBuffer.Chunks;
using DeltaZulu.DurableBuffer.Configuration;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class ChunkBuilderTests
{
    private static DurableBufferOptions DefaultOptions => new()
    {
        StoragePath = "/tmp/test",
        MaxChunkRecords = 10,
        MaxChunkBytes = 4096,
        MaxChunkAge = TimeSpan.FromSeconds(60)
    };

    [TestMethod]
    public void NewBuilder_IsEmpty()
    {
        using var builder = new ChunkBuilder(DefaultOptions);
        Assert.IsTrue(builder.IsEmpty);
        Assert.AreEqual(0, builder.RecordCount);
        Assert.IsFalse(builder.ShouldRotate);
    }

    [TestMethod]
    public void Append_IncreasesRecordCount()
    {
        using var builder = new ChunkBuilder(DefaultOptions);
        builder.Append(Encoding.UTF8.GetBytes("{\"test\":1}"));

        Assert.AreEqual(1, builder.RecordCount);
        Assert.IsFalse(builder.IsEmpty);
    }

    [TestMethod]
    public void ShouldRotate_ByCount()
    {
        var options = DefaultOptions with { MaxChunkRecords = 3 };
        using var builder = new ChunkBuilder(options);

        builder.Append("a"u8.ToArray());
        builder.Append("b"u8.ToArray());
        Assert.IsFalse(builder.ShouldRotate);

        builder.Append("c"u8.ToArray());
        Assert.IsTrue(builder.ShouldRotate);
    }

    [TestMethod]
    public void ShouldRotate_ByAge()
    {
        var options = DefaultOptions with { MaxChunkAge = TimeSpan.Zero };
        using var builder = new ChunkBuilder(options);
        builder.Append("x"u8.ToArray());
        Assert.IsTrue(builder.ShouldRotate);
    }

    [TestMethod]
    public void Seal_ProducesValidChunkData()
    {
        using var builder = new ChunkBuilder(DefaultOptions);
        builder.Append(Encoding.UTF8.GetBytes("{\"key\":\"value1\"}"));
        builder.Append(Encoding.UTF8.GetBytes("{\"key\":\"value2\"}"));

        var (data, metadata) = builder.Seal();

        Assert.IsGreaterThan(ChunkFormat.HeaderSize + ChunkFormat.FooterSize, data.Length);
        Assert.AreEqual(2, metadata.RecordCount);
        Assert.IsGreaterThan(0, metadata.PayloadBytes);
        Assert.StartsWith("sha256:", metadata.Checksum);
        Assert.IsNotNull(metadata.SealedUtc);
    }

    [TestMethod]
    public void Seal_ChunkPassesChecksumValidation()
    {
        using var builder = new ChunkBuilder(DefaultOptions);
        builder.Append(Encoding.UTF8.GetBytes("record1"));
        builder.Append(Encoding.UTF8.GetBytes("record2"));
        builder.Append(Encoding.UTF8.GetBytes("record3"));

        var (data, _) = builder.Seal();
        Assert.IsTrue(ChunkFormat.ValidateChecksum(data));
    }

    [TestMethod]
    public void Seal_RecordsAreReadable()
    {
        using var builder = new ChunkBuilder(DefaultOptions);
        var r1 = Encoding.UTF8.GetBytes("{\"id\":1}");
        var r2 = Encoding.UTF8.GetBytes("{\"id\":2}");
        builder.Append(r1);
        builder.Append(r2);

        var (data, _) = builder.Seal();
        var records = ChunkFormat.ReadRecords(data);

        Assert.HasCount(2, records);
        Assert.AreEqual("{\"id\":1}", Encoding.UTF8.GetString(records[0].Span));
        Assert.AreEqual("{\"id\":2}", Encoding.UTF8.GetString(records[1].Span));
    }

    [TestMethod]
    public void Reset_ClearsState()
    {
        using var builder = new ChunkBuilder(DefaultOptions);
        var oldId = builder.ChunkId;
        builder.Append("data"u8.ToArray());

        builder.Reset();

        Assert.IsTrue(builder.IsEmpty);
        Assert.AreEqual(0, builder.RecordCount);
        Assert.AreNotEqual(oldId, builder.ChunkId);
    }

    [TestMethod]
    public void WouldExceedLimit_SmallRecord_ReturnsFalse()
    {
        using var builder = new ChunkBuilder(DefaultOptions);
        Assert.IsFalse(builder.WouldExceedLimit(10));
    }

    [TestMethod]
    public void WouldExceedLimit_HugeRecord_ReturnsTrue()
    {
        var options = DefaultOptions with { MaxChunkBytes = 200 };
        using var builder = new ChunkBuilder(options);
        Assert.IsTrue(builder.WouldExceedLimit(200));
    }
}
