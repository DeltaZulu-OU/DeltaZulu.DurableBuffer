using System.Globalization;
using System.Text.Json;
using DeltaZulu.DurableBuffer.Chunks;
using Microsoft.Extensions.Logging;

namespace DeltaZulu.DurableBuffer.Storage;

internal sealed class FileChunkStore : IChunkStore
{
    private readonly ILogger? _logger;
    private readonly long _maxDeadLetterBytes;
    private readonly long _maxQuarantineBytes;
    private readonly Action<StoredChunk>? _onDeadLetterEvicted;
    private readonly Action<string, long>? _onQuarantineEvicted;

    public FileChunkStore(
        string basePath,
        ILogger? logger = null,
        long maxDeadLetterBytes = long.MaxValue,
        long maxQuarantineBytes = long.MaxValue,
        Action<StoredChunk>? onDeadLetterEvicted = null,
        Action<string, long>? onQuarantineEvicted = null)
    {
        SealedPath = Path.Combine(basePath, "sealed");
        DeadLetterPath = Path.Combine(basePath, "deadletter");
        QuarantinePath = Path.Combine(basePath, "quarantine");
        _logger = logger;
        _maxDeadLetterBytes = maxDeadLetterBytes;
        _maxQuarantineBytes = maxQuarantineBytes;
        _onDeadLetterEvicted = onDeadLetterEvicted;
        _onQuarantineEvicted = onQuarantineEvicted;

        EnsureDirectories(basePath);
        MigrateLegacyDispatchingDirectory(basePath);
    }

    public string SealedPath { get; }
    public string DeadLetterPath { get; }
    public string QuarantinePath { get; }

    private static void EnsureDirectories(string basePath)
    {
        Directory.CreateDirectory(Path.Combine(basePath, "active"));
        Directory.CreateDirectory(Path.Combine(basePath, "sealed"));
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

    /// <summary>
    /// Older buffer versions parked in-flight chunks in a "dispatching" directory.
    /// Move any leftovers back into "sealed" so recovery re-validates and re-queues
    /// them, then drop the legacy directory.
    /// </summary>
    private void MigrateLegacyDispatchingDirectory(string basePath)
    {
        var legacyPath = Path.Combine(basePath, "dispatching");
        if (!Directory.Exists(legacyPath))
        {
            return;
        }

        foreach (var file in Directory.EnumerateFiles(legacyPath))
        {
            var destination = Path.Combine(SealedPath, Path.GetFileName(file));
            try
            {
                EnsureNotSymlink(file);
                if (!File.Exists(destination))
                {
                    File.Move(file, destination);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to migrate legacy dispatching file: {File}", file);
            }
        }

        try
        {
            if (!Directory.EnumerateFileSystemEntries(legacyPath).Any())
            {
                Directory.Delete(legacyPath);
            }
        }
        catch
        {
            // Best effort: an undeletable empty directory is harmless.
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

    public ValueTask DeleteAsync(StoredChunk chunk, CancellationToken cancellationToken = default)
    {
        SafeDelete(chunk.ChunkFilePath);
        SafeDelete(chunk.MetadataFilePath);
        return ValueTask.CompletedTask;
    }

    public async ValueTask<StoredChunk> MoveToDeadLetterAsync(
        StoredChunk chunk,
        CancellationToken cancellationToken = default)
    {
        var moved = await MoveChunkAsync(chunk, DeadLetterPath);
        await TrimDeadLetterAsync(cancellationToken);
        return moved;
    }

    public ValueTask QuarantineAsync(string filePath, CancellationToken cancellationToken = default)
    {
        EnsureNotSymlink(filePath);
        var dest = Path.Combine(QuarantinePath,
            $"quarantined_{DateTimeOffset.UtcNow:yyyyMMddTHHmmssfffZ}_{Path.GetFileName(filePath)}");
        File.Move(filePath, dest);
        TrimQuarantine();
        return ValueTask.CompletedTask;
    }

    public ValueTask<IReadOnlyList<StoredChunk>> GetSealedChunksAsync(
        CancellationToken cancellationToken = default) =>
        ScanDirectoryAsync(SealedPath, cancellationToken);

    public ValueTask<long> GetDiskBytesUsedAsync(CancellationToken cancellationToken = default) =>
        // Dead-letter and quarantine data is abandoned data with its own bounded ring-buffer
        // budget (see GetDeadLetterBytesUsedAsync/GetQuarantineBytesUsedAsync); it must not
        // compete with live, still-retryable chunks for the buffer's backpressure quota.
        ValueTask.FromResult(GetDirectoryBytes(SealedPath));

    public ValueTask<long> GetDeadLetterBytesUsedAsync(CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(GetDirectoryBytes(DeadLetterPath));

    public ValueTask<long> GetQuarantineBytesUsedAsync(CancellationToken cancellationToken = default) =>
        ValueTask.FromResult(GetDirectoryBytes(QuarantinePath));

    private static long GetDirectoryBytes(string directory)
    {
        if (!Directory.Exists(directory))
        {
            return 0;
        }

        long total = 0;
        foreach (var file in Directory.EnumerateFiles(directory))
        {
            total += new FileInfo(file).Length;
        }
        return total;
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

    private async ValueTask TrimDeadLetterAsync(CancellationToken cancellationToken)
    {
        if (_maxDeadLetterBytes == long.MaxValue)
        {
            return;
        }

        var chunks = await ScanDirectoryAsync(DeadLetterPath, cancellationToken);
        if (chunks.Count == 0)
        {
            return;
        }

        // Evict oldest-by-original-record-age first: a chunk that was only just
        // dead-lettered after exhausting retries is still old data if its source
        // records are old, so ordering is by CreatedUtc, not by dead-letter arrival time.
        var ordered = chunks.OrderBy(static c => c.Metadata.CreatedUtc).ToList();
        var total = ordered.Sum(GetChunkFileBytes);

        var index = 0;
        while (total > _maxDeadLetterBytes && index < ordered.Count)
        {
            var evicted = ordered[index++];
            total -= GetChunkFileBytes(evicted);
            SafeDelete(evicted.ChunkFilePath);
            SafeDelete(evicted.MetadataFilePath);
            _onDeadLetterEvicted?.Invoke(evicted);
        }
    }

    private void TrimQuarantine()
    {
        if (_maxQuarantineBytes == long.MaxValue || !Directory.Exists(QuarantinePath))
        {
            return;
        }

        var files = Directory.EnumerateFiles(QuarantinePath)
            .Select(path => new FileInfo(path))
            .OrderBy(GetQuarantineTimestamp)
            .ToList();

        var total = files.Sum(f => f.Length);

        var index = 0;
        while (total > _maxQuarantineBytes && index < files.Count)
        {
            var file = files[index++];
            total -= file.Length;
            SafeDelete(file.FullName);
            _onQuarantineEvicted?.Invoke(file.FullName, file.Length);
        }
    }

    private static long GetChunkFileBytes(StoredChunk chunk)
    {
        long total = 0;
        if (File.Exists(chunk.ChunkFilePath))
        {
            total += new FileInfo(chunk.ChunkFilePath).Length;
        }

        if (File.Exists(chunk.MetadataFilePath))
        {
            total += new FileInfo(chunk.MetadataFilePath).Length;
        }

        return total;
    }

    private static DateTimeOffset GetQuarantineTimestamp(FileInfo file)
    {
        const string prefix = "quarantined_";
        if (file.Name.StartsWith(prefix, StringComparison.Ordinal))
        {
            var afterPrefix = file.Name[prefix.Length..];
            var separatorIndex = afterPrefix.IndexOf('_');
            var token = separatorIndex >= 0 ? afterPrefix[..separatorIndex] : afterPrefix;
            if (DateTimeOffset.TryParseExact(
                token, "yyyyMMddTHHmmssfffZ", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var parsed))
            {
                return parsed;
            }
        }

        return file.LastWriteTimeUtc;
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