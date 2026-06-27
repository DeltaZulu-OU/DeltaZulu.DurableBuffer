using System.Text;
using System.Text.Json;
using DeltaZulu.DurableBuffer.Chunks;
using DeltaZulu.DurableBuffer.Configuration;
using DeltaZulu.DurableBuffer.Storage;

namespace DeltaZulu.DurableBuffer.Tests;

[TestClass]
public sealed class FileChunkStoreTests
{
    private string _basePath = null!;
    private FileChunkStore _store = null!;

    [TestInitialize]
    public void Setup()
    {
        _basePath = Path.Combine(Path.GetTempPath(), $"dz_test_{Guid.NewGuid():N}");
        _store = new FileChunkStore(_basePath);
    }

    [TestCleanup]
    public void Cleanup()
    {
        try { Directory.Delete(_basePath, true); }
        catch { }
    }

    private static (byte[] Data, ChunkMetadata Meta) BuildSealedChunk(string chunkId, params string[] records)
    {
        var options = new DurableBufferOptions
        {
            StoragePath = "/tmp/dummy",
            MaxChunkRecords = 1000,
            MaxChunkBytes = 1024 * 1024,
            MaxChunkAge = TimeSpan.FromMinutes(5)
        };

        using var builder = new ChunkBuilder(options);
        foreach (var r in records)
        {
            builder.Append(Encoding.UTF8.GetBytes(r));
        }

        var (data, meta) = builder.Seal();
        return (data, meta with { ChunkId = chunkId });
    }

    [TestMethod]
    public void Constructor_CreatesDirectories()
    {
        Assert.IsTrue(Directory.Exists(Path.Combine(_basePath, "active")));
        Assert.IsTrue(Directory.Exists(Path.Combine(_basePath, "sealed")));
        Assert.IsTrue(Directory.Exists(Path.Combine(_basePath, "dispatching")));
        Assert.IsTrue(Directory.Exists(Path.Combine(_basePath, "deadletter")));
        Assert.IsTrue(Directory.Exists(Path.Combine(_basePath, "quarantine")));
    }

    [TestMethod]
    public async Task SealAsync_CreatesChunkAndMetadataFiles()
    {
        var chunkId = ChunkId.NewChunkId();
        var (data, meta) = BuildSealedChunk(chunkId.Value, "hello", "world");

        var stored = await _store.SealAsync(chunkId, data, meta);

        Assert.IsTrue(File.Exists(stored.ChunkFilePath));
        Assert.IsTrue(File.Exists(stored.MetadataFilePath));
        Assert.Contains("sealed", stored.ChunkFilePath);
        Assert.AreEqual(chunkId, stored.Id);

        var savedData = await File.ReadAllBytesAsync(stored.ChunkFilePath);
        CollectionAssert.AreEqual(data, savedData);
    }

    [TestMethod]
    public async Task SealAsync_MetadataIsValidJson()
    {
        var chunkId = ChunkId.NewChunkId();
        var (data, meta) = BuildSealedChunk(chunkId.Value, "test");

        var stored = await _store.SealAsync(chunkId, data, meta);

        var json = await File.ReadAllBytesAsync(stored.MetadataFilePath);
        var deserialized = JsonSerializer.Deserialize<ChunkMetadata>(json);
        Assert.IsNotNull(deserialized);
        Assert.AreEqual(meta.RecordCount, deserialized.RecordCount);
    }

    [TestMethod]
    public async Task MoveToDispatchingAsync_MovesFiles()
    {
        var chunkId = ChunkId.NewChunkId();
        var (data, meta) = BuildSealedChunk(chunkId.Value, "record1");
        var sealed_ = await _store.SealAsync(chunkId, data, meta);

        var dispatching = await _store.MoveToDispatchingAsync(sealed_);

        Assert.IsFalse(File.Exists(sealed_.ChunkFilePath));
        Assert.IsTrue(File.Exists(dispatching.ChunkFilePath));
        Assert.Contains("dispatching", dispatching.ChunkFilePath);
    }

    [TestMethod]
    public async Task MoveToDeadLetterAsync_MovesFiles()
    {
        var chunkId = ChunkId.NewChunkId();
        var (data, meta) = BuildSealedChunk(chunkId.Value, "record1");
        var sealed_ = await _store.SealAsync(chunkId, data, meta);

        var deadLettered = await _store.MoveToDeadLetterAsync(sealed_);

        Assert.IsFalse(File.Exists(sealed_.ChunkFilePath));
        Assert.IsTrue(File.Exists(deadLettered.ChunkFilePath));
        Assert.Contains("deadletter", deadLettered.ChunkFilePath);
    }

    [TestMethod]
    public async Task MoveToSealedAsync_UpdatesMetadata()
    {
        var chunkId = ChunkId.NewChunkId();
        var (data, meta) = BuildSealedChunk(chunkId.Value, "record1");
        var sealed_ = await _store.SealAsync(chunkId, data, meta);
        var dispatching = await _store.MoveToDispatchingAsync(sealed_);

        var updatedMeta = dispatching.Metadata with { AttemptCount = 3, LastError = "timeout" };
        var backToSealed = await _store.MoveToSealedAsync(dispatching, updatedMeta);

        Assert.Contains("sealed", backToSealed.ChunkFilePath);
        Assert.AreEqual(3, backToSealed.Metadata.AttemptCount);
        Assert.AreEqual("timeout", backToSealed.Metadata.LastError);
    }

    [TestMethod]
    public async Task DeleteAsync_RemovesFiles()
    {
        var chunkId = ChunkId.NewChunkId();
        var (data, meta) = BuildSealedChunk(chunkId.Value, "record1");
        var stored = await _store.SealAsync(chunkId, data, meta);

        await _store.DeleteAsync(stored);

        Assert.IsFalse(File.Exists(stored.ChunkFilePath));
        Assert.IsFalse(File.Exists(stored.MetadataFilePath));
    }

    [TestMethod]
    public async Task GetSealedChunksAsync_ReturnsSealedChunks()
    {
        var id1 = ChunkId.NewChunkId();
        var id2 = ChunkId.NewChunkId();
        var (data1, meta1) = BuildSealedChunk(id1.Value, "a");
        var (data2, meta2) = BuildSealedChunk(id2.Value, "b");

        await _store.SealAsync(id1, data1, meta1);
        await _store.SealAsync(id2, data2, meta2);

        var chunks = await _store.GetSealedChunksAsync();
        Assert.HasCount(2, chunks);
    }

    [TestMethod]
    public async Task GetDispatchingChunksAsync_ReturnsDispatchingChunks()
    {
        var chunkId = ChunkId.NewChunkId();
        var (data, meta) = BuildSealedChunk(chunkId.Value, "a");
        var sealed_ = await _store.SealAsync(chunkId, data, meta);
        await _store.MoveToDispatchingAsync(sealed_);

        var chunks = await _store.GetDispatchingChunksAsync();
        Assert.HasCount(1, chunks);
    }

    [TestMethod]
    public async Task GetDiskBytesUsedAsync_ReturnsCorrectTotal()
    {
        var chunkId = ChunkId.NewChunkId();
        var (data, meta) = BuildSealedChunk(chunkId.Value, "hello", "world");
        await _store.SealAsync(chunkId, data, meta);

        var bytesUsed = await _store.GetDiskBytesUsedAsync();
        Assert.IsGreaterThan(0, bytesUsed);
    }

    [TestMethod]
    public async Task QuarantineAsync_MovesToQuarantineDir()
    {
        var filePath = Path.Combine(_basePath, "active", "test.tmp");
        await File.WriteAllTextAsync(filePath, "junk");

        await _store.QuarantineAsync(filePath);

        Assert.IsFalse(File.Exists(filePath));
        var quarantineFiles = Directory.GetFiles(Path.Combine(_basePath, "quarantine"));
        Assert.HasCount(1, quarantineFiles);
    }

    [TestMethod]
    public async Task GetSealedChunksAsync_SkipsOrphanMetadata()
    {
        var metaFile = Path.Combine(_basePath, "sealed", "orphan.meta.json");
        var meta = new ChunkMetadata
        {
            ChunkId = "orphan",
            CreatedUtc = DateTimeOffset.UtcNow,
            RecordCount = 1,
            PayloadBytes = 10,
            Checksum = "sha256:abc"
        };
        await File.WriteAllBytesAsync(metaFile, JsonSerializer.SerializeToUtf8Bytes(meta));

        var chunks = await _store.GetSealedChunksAsync();
        Assert.IsEmpty(chunks);
    }
}
