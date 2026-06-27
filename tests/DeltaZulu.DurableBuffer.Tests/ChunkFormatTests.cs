using System.Text;
using DeltaZulu.DurableBuffer.Chunks;
using DeltaZulu.DurableBuffer.Configuration;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class ChunkFormatTests
{
    private static byte[] BuildTestChunk(params string[] records)
    {
        var options = new DurableBufferOptions {
            StoragePath = "/tmp/test",
            MaxChunkRecords = 1000,
            MaxChunkBytes = 1024 * 1024,
            MaxChunkAge = TimeSpan.FromMinutes(5)
        };

        using var builder = new ChunkBuilder(options);
        foreach (var r in records)
        {
            builder.Append(Encoding.UTF8.GetBytes(r));
        }

        var (data, _) = builder.Seal();
        return data;
    }

    [TestMethod]
    public void ValidateChecksum_ValidChunk_ReturnsTrue()
    {
        var data = BuildTestChunk("hello", "world");
        Assert.IsTrue(ChunkFormat.ValidateChecksum(data));
    }

    [TestMethod]
    public void ValidateChecksum_CorruptedChunk_ReturnsFalse()
    {
        var data = BuildTestChunk("hello", "world");
        data[ChunkFormat.HeaderSize + 5] ^= 0xFF;
        Assert.IsFalse(ChunkFormat.ValidateChecksum(data));
    }

    [TestMethod]
    public void ValidateChecksum_TooSmall_ReturnsFalse() => Assert.IsFalse(ChunkFormat.ValidateChecksum(new byte[10]));

    [TestMethod]
    public void ReadRecords_ReturnsCorrectData()
    {
        var data = BuildTestChunk("alpha", "beta", "gamma");
        var records = ChunkFormat.ReadRecords(data);

        Assert.HasCount(3, records);
        Assert.AreEqual("alpha", Encoding.UTF8.GetString(records[0].Span));
        Assert.AreEqual("beta", Encoding.UTF8.GetString(records[1].Span));
        Assert.AreEqual("gamma", Encoding.UTF8.GetString(records[2].Span));
    }

    [TestMethod]
    public void ReadRecords_InvalidMagic_Throws()
    {
        var data = BuildTestChunk("test");
        data[0] = 0xFF;

        Assert.ThrowsExactly<InvalidDataException>(
            () => ChunkFormat.ReadRecords(data));

    }

    [TestMethod]
    public void ReadRecords_TooSmall_Throws() => Assert.ThrowsExactly<InvalidDataException>(
            () => ChunkFormat.ReadRecords(new byte[10]));

    [TestMethod]
    public void ReadRecords_EmptyChunk_ReturnsEmpty()
    {
        var options = new DurableBufferOptions {
            StoragePath = "/tmp/test",
            MaxChunkRecords = 1000,
            MaxChunkBytes = 1024 * 1024,
            MaxChunkAge = TimeSpan.FromMinutes(5)
        };
        using var builder = new ChunkBuilder(options);
        var (data, _) = builder.Seal();
        var records = ChunkFormat.ReadRecords(data);
        Assert.IsEmpty(records);
    }
}
