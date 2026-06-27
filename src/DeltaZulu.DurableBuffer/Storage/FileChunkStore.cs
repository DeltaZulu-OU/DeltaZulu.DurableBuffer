using System.Text.Json;
using DeltaZulu.DurableBuffer.Chunks;
using Microsoft.Extensions.Logging;

namespace DeltaZulu.DurableBuffer.Storage;

internal sealed class FileChunkStore : IChunkStore
{
    private readonly ILogger? _logger;

    public FileChunkStore(string basePath, ILogger? logger = null)
    {
        SealedPath = Path.Combine(basePath, "sealed");
        DispatchingPath = Path.Combine(basePath, "dispatching");
        DeadLetterPath = Path.Combine(basePath, "deadletter");
        QuarantinePath = Path.Combine(basePath, "quarantine");
        _logger = logger;

        EnsureDirectories(basePath);
    }

    public string SealedPath { get; }
    public string DispatchingPath { get; }
    public string DeadLetterPath { get; }
    public string QuarantinePath { get; }

    private static void EnsureDirectories(string basePath)
    {
        Directory.CreateDirectory(Path.Combine(basePath, "active"));
        Directory.CreateDirectory(Path.Combine(basePath, "sealed"));
        Directory.CreateDirectory(Path.Combine(basePath, "dispatching"));
        Directory.CreateDirectory(Path.Combine(basePath, "deadletter"));
        Directory.CreateDirectory(Path.Combine(basePath, "quarantine"));

        if (!OperatingSystem.IsWindows())
        {
            try
            {
                File.SetUnixFileMode(basePath,
                    UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            }
            catch
            {
                // Best-effort permission hardening
            }
        }
    }

    public async ValueTask<StoredChunk> SealAsync(
        ChunkId chunkId,
        ReadOnlyMemory<byte> chunkData,
        ChunkMetadata metadata,
        CancellationToken cancellationToken = default)
    {
        var chunkFile = Path.Combine(SealedPath, $"{chunkId.Value}.chunk");
        var metaFile = Path.Combine(SealedPath, $"{chunkId.Value}.meta.json");
        var chunkTmp = chunkFile + ".tmp";
        var metaTmp = metaFile + ".tmp";

        await using (var fs = new FileStream(chunkTmp, FileMode.Create, FileAccess.Write,
            FileShare.None, 65536, FileOptions.Asynchronous | FileOptions.SequentialScan))
        {
            await fs.WriteAsync(chunkData, cancellationToken);
            await fs.FlushAsync(cancellationToken);
        }

        var metaJson = JsonSerializer.SerializeToUtf8Bytes(metadata);
        await File.WriteAllBytesAsync(metaTmp, metaJson, cancellationToken);

        File.Move(chunkTmp, chunkFile);
        File.Move(metaTmp, metaFile);

        return new StoredChunk {
            Id = chunkId,
            ChunkFilePath = chunkFile,
            MetadataFilePath = metaFile,
            Metadata = metadata
        };
    }

    public ValueTask<StoredChunk> MoveToDispatchingAsync(
        StoredChunk chunk,
        CancellationToken cancellationToken = default) =>
        MoveChunkAsync(chunk, DispatchingPath);

    public async ValueTask<StoredChunk> MoveToSealedAsync(
        StoredChunk chunk,
        ChunkMetadata updatedMetadata,
        CancellationToken cancellationToken = default)
    {
        var metaJson = JsonSerializer.SerializeToUtf8Bytes(updatedMetadata);
        await File.WriteAllBytesAsync(chunk.MetadataFilePath, metaJson, cancellationToken);
        return await MoveChunkAsync(chunk with { Metadata = updatedMetadata }, SealedPath);
    }

    public ValueTask DeleteAsync(StoredChunk chunk, CancellationToken cancellationToken = default)
    {
        SafeDelete(chunk.ChunkFilePath);
        SafeDelete(chunk.MetadataFilePath);
        return ValueTask.CompletedTask;
    }

    public ValueTask<StoredChunk> MoveToDeadLetterAsync(
        StoredChunk chunk,
        CancellationToken cancellationToken = default) =>
        MoveChunkAsync(chunk, DeadLetterPath);

    public ValueTask QuarantineAsync(string filePath, CancellationToken cancellationToken = default)
    {
        EnsureNotSymlink(filePath);
        var dest = Path.Combine(QuarantinePath,
            $"quarantined_{DateTimeOffset.UtcNow:yyyyMMddTHHmmssfffZ}_{Path.GetFileName(filePath)}");
        File.Move(filePath, dest);
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<StoredChunk>> GetSealedChunksAsync(
        CancellationToken cancellationToken = default) =>
        ScanDirectoryAsync(SealedPath, cancellationToken);

    public ValueTask<IReadOnlyList<StoredChunk>> GetDispatchingChunksAsync(
        CancellationToken cancellationToken = default) =>
        ScanDirectoryAsync(DispatchingPath, cancellationToken);

    public ValueTask<long> GetDiskBytesUsedAsync(CancellationToken cancellationToken = default)
    {
        long total = 0;
        foreach (var dir in new[] { SealedPath, DispatchingPath, DeadLetterPath, QuarantinePath })
        {
            if (!Directory.Exists(dir))
            {
                continue;
            }

            foreach (var file in Directory.EnumerateFiles(dir))
            {
                total += new FileInfo(file).Length;
            }
        }
        return ValueTask.FromResult(total);
    }

    private ValueTask<StoredChunk> MoveChunkAsync(StoredChunk chunk, string targetDir)
    {
        EnsureNotSymlink(chunk.ChunkFilePath);
        EnsureNotSymlink(chunk.MetadataFilePath);

        var targetChunk = Path.Combine(targetDir, Path.GetFileName(chunk.ChunkFilePath));
        var targetMeta = Path.Combine(targetDir, Path.GetFileName(chunk.MetadataFilePath));

        File.Move(chunk.ChunkFilePath, targetChunk);
        File.Move(chunk.MetadataFilePath, targetMeta);

        return ValueTask.FromResult(chunk with {
            ChunkFilePath = targetChunk,
            MetadataFilePath = targetMeta
        });
    }

    private async ValueTask<IReadOnlyList<StoredChunk>> ScanDirectoryAsync(
        string directory,
        CancellationToken cancellationToken)
    {
        var chunks = new List<StoredChunk>();
        if (!Directory.Exists(directory))
        {
            return chunks;
        }

        foreach (var metaFile in Directory.EnumerateFiles(directory, "*.meta.json"))
        {
            try
            {
                var json = await File.ReadAllBytesAsync(metaFile, cancellationToken);
                var metadata = JsonSerializer.Deserialize<ChunkMetadata>(json);
                if (metadata is null)
                {
                    continue;
                }

                var chunkFile = metaFile.Replace(".meta.json", ".chunk", StringComparison.Ordinal);
                if (!File.Exists(chunkFile))
                {
                    continue;
                }

                chunks.Add(new StoredChunk {
                    Id = new ChunkId(metadata.ChunkId),
                    ChunkFilePath = chunkFile,
                    MetadataFilePath = metaFile,
                    Metadata = metadata
                });
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Skipping unreadable metadata: {File}", metaFile);
            }
        }

        return chunks;
    }

    private static void EnsureNotSymlink(string path)
    {
        if (!File.Exists(path))
        {
            return;
        }

        var info = new FileInfo(path);
        if (info.Attributes.HasFlag(FileAttributes.ReparsePoint))
        {
            throw new InvalidOperationException($"Refusing to operate on symlink: {path}");
        }
    }

    private static void SafeDelete(string path)
    {
        EnsureNotSymlink(path);
        try { File.Delete(path); }
        catch { /* best effort */ }
    }
}