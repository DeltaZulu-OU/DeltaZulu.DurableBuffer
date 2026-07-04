using System.Threading.Channels;
using DeltaZulu.DurableBuffer.Chunks;
using DeltaZulu.DurableBuffer.Metrics;
using DeltaZulu.DurableBuffer.Storage;
using Microsoft.Extensions.Logging;

namespace DeltaZulu.DurableBuffer.Recovery;

internal sealed class FileSystemRecoveryManager : IRecoveryManager
{
    private readonly string _basePath;
    private readonly ChannelWriter<StoredChunk> _dispatchWriter;
    private readonly BufferEventBroadcaster _events;
    private readonly ILogger? _logger;
    private readonly BufferMetricsCounter _metrics;
    private readonly IChunkStore _store;

    public FileSystemRecoveryManager(
        IChunkStore store,
        ChannelWriter<StoredChunk> dispatchWriter,
        BufferMetricsCounter metrics,
        BufferEventBroadcaster events,
        string basePath,
        ILogger? logger = null)
    {
        _store = store;
        _dispatchWriter = dispatchWriter;
        _metrics = metrics;
        _events = events;
        _basePath = basePath;
        _logger = logger;
    }

    public async ValueTask<RecoverySummary> RecoverAsync(CancellationToken cancellationToken = default)
    {
        _events.Publish(BufferEvent.Create(BufferEventType.BufferRecoveryStarted));

        var recovered = 0;
        var quarantined = 0;
        const int deadLettered = 0;
        long estimatedLost = 0;

        quarantined += await QuarantineActiveChunksAsync(cancellationToken);
        estimatedLost += quarantined > 0 ? -1 : 0; // unknown record count

        var sealedChunks = await _store.GetSealedChunksAsync(cancellationToken);
        foreach (var chunk in sealedChunks)
        {
            try
            {
                if (!await ValidateChunkAsync(chunk, cancellationToken))
                {
                    await _store.QuarantineAsync(chunk.ChunkFilePath, cancellationToken);
                    SafeDelete(chunk.MetadataFilePath);
                    quarantined++;
                    _metrics.ChunkQuarantined();
                    _events.Publish(BufferEvent.Create(
                        BufferEventType.BufferFileQuarantined, chunk.Id.Value,
                        detail: "Checksum mismatch on recovery"));
                    continue;
                }

                await _dispatchWriter.WriteAsync(chunk, cancellationToken);
                recovered++;
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to recover sealed chunk {ChunkId}", chunk.Id);
                quarantined++;
            }
        }

        quarantined += await QuarantineOrphanFilesAsync(cancellationToken);

        var summary = new RecoverySummary {
            RecoveredChunks = recovered,
            QuarantinedFiles = quarantined,
            DeadLetteredChunks = deadLettered,
            EstimatedLostRecords = estimatedLost < 0 ? 0 : estimatedLost
        };

        _events.Publish(BufferEvent.Create(
            BufferEventType.BufferRecoveryCompleted,
            detail: $"Recovered={recovered}, Quarantined={quarantined}"));

        _logger?.LogInformation(
            "Buffer recovery complete: {Recovered} recovered, {Quarantined} quarantined",
            recovered, quarantined);

        return summary;
    }

    private static void SafeDelete(string path)
    {
        try { File.Delete(path); }
        catch { }
    }

    private static async Task<bool> ValidateChunkAsync(StoredChunk chunk, CancellationToken cancellationToken)
    {
        var data = await File.ReadAllBytesAsync(chunk.ChunkFilePath, cancellationToken);
        return ChunkFormat.ValidateChecksum(data);
    }

    private async Task<int> QuarantineActiveChunksAsync(CancellationToken cancellationToken)
    {
        var activePath = Path.Combine(_basePath, "active");
        if (!Directory.Exists(activePath))
        {
            return 0;
        }

        var count = 0;
        foreach (var file in Directory.EnumerateFiles(activePath, "*.tmp"))
        {
            try
            {
                await _store.QuarantineAsync(file, cancellationToken);
                count++;
                _metrics.ChunkQuarantined();
                _events.Publish(BufferEvent.Create(
                    BufferEventType.BufferFileQuarantined,
                    detail: $"Active chunk quarantined: {Path.GetFileName(file)}"));
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "Failed to quarantine active file: {File}", file);
            }
        }

        return count;
    }

    private async Task<int> QuarantineOrphanFilesAsync(CancellationToken cancellationToken)
    {
        var count = 0;
        foreach (var dir in new[] { Path.Combine(_basePath, "sealed") })
        {
            if (!Directory.Exists(dir))
            {
                continue;
            }

            foreach (var chunkFile in Directory.EnumerateFiles(dir, "*.chunk"))
            {
                var metaFile = chunkFile.Replace(".chunk", ".meta.json", StringComparison.Ordinal);
                if (File.Exists(metaFile))
                {
                    continue;
                }

                try
                {
                    await _store.QuarantineAsync(chunkFile, cancellationToken);
                    count++;
                    _metrics.ChunkQuarantined();
                    _events.Publish(BufferEvent.Create(
                        BufferEventType.BufferFileQuarantined,
                        detail: $"Orphan chunk (no metadata): {Path.GetFileName(chunkFile)}"));
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Failed to quarantine orphan file: {File}", chunkFile);
                }
            }

            foreach (var metaFile in Directory.EnumerateFiles(dir, "*.meta.json"))
            {
                var chunkFile = metaFile.Replace(".meta.json", ".chunk", StringComparison.Ordinal);
                if (File.Exists(chunkFile))
                {
                    continue;
                }

                try
                {
                    SafeDelete(metaFile);
                    count++;
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Failed to clean orphan metadata: {File}", metaFile);
                }
            }
        }

        return count;
    }
}