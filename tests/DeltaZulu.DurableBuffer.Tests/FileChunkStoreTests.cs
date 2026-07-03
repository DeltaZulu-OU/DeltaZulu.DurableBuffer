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

        var stored = await _store.SealAsync(chunkId, data, meta, TestContext.CancellationToken);

        Assert.IsTrue(File.Exists(stored.ChunkFilePath));
        Assert.IsTrue(File.Exists(stored.MetadataFilePath));
        Assert.Contains("sealed", stored.ChunkFilePath);
        Assert.AreEqual(chunkId, stored.Id);

        var savedData = await File.ReadAllBytesAsync(stored.ChunkFilePath, TestContext.CancellationToken);
        CollectionAssert.AreEqual(data, savedData);
    }

    [TestMethod]
    public async Task SealAsync_MetadataIsValidJson()
    {
        var chunkId = ChunkId.NewChunkId();
        var (data, meta) = BuildSealedChunk(chunkId.Value, "test");

        var stored = await _store.SealAsync(chunkId, data, meta, TestContext.CancellationToken);

        var json = await File.ReadAllBytesAsync(stored.MetadataFilePath, TestContext.CancellationToken);
        var deserialized = JsonSerializer.Deserialize<ChunkMetadata>(json);
        Assert.IsNotNull(deserialized);
        Assert.AreEqual(meta.RecordCount, deserialized.RecordCount);
    }

    [TestMethod]
    public async Task MoveToDispatchingAsync_MovesFiles()
    {
        var chunkId = ChunkId.NewChunkId();
        var (data, meta) = BuildSealedChunk(chunkId.Value, "record1");
        var sealed_ = await _store.SealAsync(chunkId, data, meta, TestContext.CancellationToken);

        var dispatching = await _store.MoveToDispatchingAsync(sealed_, TestContext.CancellationToken);

        Assert.IsFalse(File.Exists(sealed_.ChunkFilePath));
        Assert.IsTrue(File.Exists(dispatching.ChunkFilePath));
        Assert.Contains("dispatching", dispatching.ChunkFilePath);
    }

    [TestMethod]
    public async Task MoveToDeadLetterAsync_MovesFiles()
    {
        var chunkId = ChunkId.NewChunkId();
        var (data, meta) = BuildSealedChunk(chunkId.Value, "record1");
        var sealed_ = await _store.SealAsync(chunkId, data, meta, TestContext.CancellationToken);

        var deadLettered = await _store.MoveToDeadLetterAsync(sealed_, TestContext.CancellationToken);

        Assert.IsFalse(File.Exists(sealed_.ChunkFilePath));
        Assert.IsTrue(File.Exists(deadLettered.ChunkFilePath));
        Assert.Contains("deadletter", deadLettered.ChunkFilePath);
    }

    [TestMethod]
    public async Task MoveToSealedAsync_UpdatesMetadata()
    {
        var chunkId = ChunkId.NewChunkId();
        var (data, meta) = BuildSealedChunk(chunkId.Value, "record1");
        var sealed_ = await _store.SealAsync(chunkId, data, meta, TestContext.CancellationToken);
        var dispatching = await _store.MoveToDispatchingAsync(sealed_, TestContext.CancellationToken);

        var updatedMeta = dispatching.Metadata with { AttemptCount = 3, LastError = "timeout" };
        var backToSealed = await _store.MoveToSealedAsync(dispatching, updatedMeta, TestContext.CancellationToken);

        Assert.Contains("sealed", backToSealed.ChunkFilePath);
        Assert.AreEqual(3, backToSealed.Metadata.AttemptCount);
        Assert.AreEqual("timeout", backToSealed.Metadata.LastError);
    }

    [TestMethod]
    public async Task DeleteAsync_RemovesFiles()
    {
        var chunkId = ChunkId.NewChunkId();
        var (data, meta) = BuildSealedChunk(chunkId.Value, "record1");
        var stored = await _store.SealAsync(chunkId, data, meta, TestContext.CancellationToken);

        await _store.DeleteAsync(stored, TestContext.CancellationToken);

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

        await _store.SealAsync(id1, data1, meta1, TestContext.CancellationToken);
        await _store.SealAsync(id2, data2, meta2, TestContext.CancellationToken);

        var chunks = await _store.GetSealedChunksAsync(TestContext.CancellationToken);
        Assert.HasCount(2, chunks);
    }

    [TestMethod]
    public async Task GetDispatchingChunksAsync_ReturnsDispatchingChunks()
    {
        var chunkId = ChunkId.NewChunkId();
        var (data, meta) = BuildSealedChunk(chunkId.Value, "a");
        var sealed_ = await _store.SealAsync(chunkId, data, meta, TestContext.CancellationToken);
        await _store.MoveToDispatchingAsync(sealed_, TestContext.CancellationToken);

        var chunks = await _store.GetDispatchingChunksAsync(TestContext.CancellationToken);
        Assert.HasCount(1, chunks);
    }

    [TestMethod]
    public async Task GetDiskBytesUsedAsync_ReturnsCorrectTotal()
    {
        var chunkId = ChunkId.NewChunkId();
        var (data, meta) = BuildSealedChunk(chunkId.Value, "hello", "world");
        await _store.SealAsync(chunkId, data, meta, TestContext.CancellationToken);

        var bytesUsed = await _store.GetDiskBytesUsedAsync(TestContext.CancellationToken);
        Assert.IsGreaterThan(0, bytesUsed);
    }

    [TestMethod]
    public async Task QuarantineAsync_MovesToQuarantineDir()
    {
        var filePath = Path.Combine(_basePath, "active", "test.tmp");
        await File.WriteAllTextAsync(filePath, "junk", TestContext.CancellationToken);

        await _store.QuarantineAsync(filePath, TestContext.CancellationToken);

        Assert.IsFalse(File.Exists(filePath));
        var quarantineFiles = Directory.GetFiles(Path.Combine(_basePath, "quarantine"));
        Assert.HasCount(1, quarantineFiles);
    }

    [TestMethod]
    public async Task GetDiskBytesUsedAsync_ExcludesDeadLetterAndQuarantine()
    {
        var chunkId = ChunkId.NewChunkId();
        var (data, meta) = BuildSealedChunk(chunkId.Value, "hello");
        var sealed_ = await _store.SealAsync(chunkId, data, meta, TestContext.CancellationToken);
        await _store.MoveToDeadLetterAsync(sealed_, TestContext.CancellationToken);

        var quarantineFile = Path.Combine(_basePath, "active", "corrupt.tmp");
        await File.WriteAllTextAsync(quarantineFile, "junk", TestContext.CancellationToken);
        await _store.QuarantineAsync(quarantineFile, TestContext.CancellationToken);

        var liveBytes = await _store.GetDiskBytesUsedAsync(TestContext.CancellationToken);
        Assert.AreEqual(0, liveBytes);

        var deadLetterBytes = await _store.GetDeadLetterBytesUsedAsync(TestContext.CancellationToken);
        Assert.IsGreaterThan(0, deadLetterBytes);

        var quarantineBytes = await _store.GetQuarantineBytesUsedAsync(TestContext.CancellationToken);
        Assert.IsGreaterThan(0, quarantineBytes);
    }

    [TestMethod]
    public async Task MoveToDeadLetterAsync_EvictsOldestByOriginalAgeWhenOverCapacity()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"dz_test_{Guid.NewGuid():N}");
        try
        {
            var oldId = ChunkId.NewChunkId();
            var (oldData, oldMetaBase) = BuildSealedChunk(oldId.Value, "old-record");
            var oldMeta = oldMetaBase with { CreatedUtc = DateTimeOffset.UtcNow.AddDays(-1) };
            var singleChunkBytes = oldData.Length + JsonSerializer.SerializeToUtf8Bytes(oldMeta).Length;

            var evicted = new List<StoredChunk>();
            var store = new FileChunkStore(
                basePath,
                maxDeadLetterBytes: singleChunkBytes + 16,
                onDeadLetterEvicted: evicted.Add);

            var oldSealed = await store.SealAsync(oldId, oldData, oldMeta, TestContext.CancellationToken);
            await store.MoveToDeadLetterAsync(oldSealed, TestContext.CancellationToken);

            var newId = ChunkId.NewChunkId();
            var (newData, newMeta) = BuildSealedChunk(newId.Value, "new-record");
            var newSealed = await store.SealAsync(newId, newData, newMeta, TestContext.CancellationToken);
            await store.MoveToDeadLetterAsync(newSealed, TestContext.CancellationToken);

            Assert.HasCount(1, evicted);
            Assert.AreEqual(oldId, evicted[0].Id);

            var remainingFiles = Directory.GetFiles(Path.Combine(basePath, "deadletter"));
            Assert.HasCount(2, remainingFiles);
            Assert.IsTrue(remainingFiles.Any(f => f.Contains(newId.Value, StringComparison.Ordinal)));
        }
        finally
        {
            try { Directory.Delete(basePath, true); } catch { }
        }
    }

    [TestMethod]
    public async Task QuarantineAsync_EvictsOldestWhenOverCapacity()
    {
        var basePath = Path.Combine(Path.GetTempPath(), $"dz_test_{Guid.NewGuid():N}");
        try
        {
            var evicted = new List<(string Path, long Bytes)>();
            var store = new FileChunkStore(
                basePath,
                maxQuarantineBytes: 10,
                onQuarantineEvicted: (path, bytes) => evicted.Add((path, bytes)));

            var file1 = Path.Combine(basePath, "active", "first.tmp");
            await File.WriteAllTextAsync(file1, "12345", TestContext.CancellationToken);
            await store.QuarantineAsync(file1, TestContext.CancellationToken);

            await Task.Delay(20, TestContext.CancellationToken);

            var file2 = Path.Combine(basePath, "active", "second.tmp");
            await File.WriteAllTextAsync(file2, "1234567890", TestContext.CancellationToken);
            await store.QuarantineAsync(file2, TestContext.CancellationToken);

            Assert.HasCount(1, evicted);
            Assert.Contains("first.tmp", evicted[0].Path);

            var remaining = Directory.GetFiles(Path.Combine(basePath, "quarantine"));
            Assert.HasCount(1, remaining);
            Assert.Contains("second.tmp", remaining[0]);
        }
        finally
        {
            try { Directory.Delete(basePath, true); } catch { }
        }
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
        await File.WriteAllBytesAsync(metaFile, JsonSerializer.SerializeToUtf8Bytes(meta), TestContext.CancellationToken);

        var chunks = await _store.GetSealedChunksAsync(TestContext.CancellationToken);
        Assert.IsEmpty(chunks);
    }

    public TestContext TestContext { get; set; }
}
