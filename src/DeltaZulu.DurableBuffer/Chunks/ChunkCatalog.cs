namespace DeltaZulu.DurableBuffer.Chunks;

internal sealed class ChunkCatalog
{
    private readonly object _sync = new();
    private readonly Dictionary<string, Entry> _entries = new(StringComparer.Ordinal);
    private readonly PriorityQueue<string, long> _available = new();

    public void AddAvailable(StoredChunk chunk)
    {
        lock (_sync)
        {
            _entries[chunk.Id.Value] = new Entry(chunk, ChunkCatalogState.Available);
            _available.Enqueue(chunk.Id.Value, ToPriority(chunk.Metadata.CreatedUtc));
        }
    }

    public bool TryDequeueAvailable(out StoredChunk chunk)
    {
        lock (_sync)
        {
            while (_available.TryDequeue(out var chunkId, out _))
            {
                if (!_entries.TryGetValue(chunkId, out var entry) || entry.State != ChunkCatalogState.Available)
                {
                    continue;
                }

                entry.State = ChunkCatalogState.Enqueued;
                chunk = entry.Chunk;
                return true;
            }
        }

        chunk = null!;
        return false;
    }

    public bool TryMarkAvailable(StoredChunk chunk)
    {
        lock (_sync)
        {
            if (!_entries.TryGetValue(chunk.Id.Value, out var entry) || entry.State == ChunkCatalogState.Terminal)
            {
                return false;
            }

            entry.State = ChunkCatalogState.Available;
            _available.Enqueue(chunk.Id.Value, ToPriority(entry.Chunk.Metadata.CreatedUtc));
            return true;
        }
    }

    public bool TryMarkTerminal(StoredChunk chunk)
    {
        lock (_sync)
        {
            if (!_entries.TryGetValue(chunk.Id.Value, out var entry))
            {
                return false;
            }

            entry.State = ChunkCatalogState.Terminal;
            return true;
        }
    }

    public bool TryRemoveOldestAvailable(out StoredChunk chunk)
    {
        lock (_sync)
        {
            while (_available.TryDequeue(out var chunkId, out _))
            {
                if (!_entries.TryGetValue(chunkId, out var entry) || entry.State != ChunkCatalogState.Available)
                {
                    continue;
                }

                entry.State = ChunkCatalogState.Terminal;
                chunk = entry.Chunk;
                return true;
            }
        }

        chunk = null!;
        return false;
    }

    public ChunkCatalogSnapshot Snapshot()
    {
        lock (_sync)
        {
            var now = DateTimeOffset.UtcNow;
            var available = _entries.Values.Where(static entry => entry.State == ChunkCatalogState.Available).ToList();
            var enqueued = _entries.Values.Where(static entry => entry.State == ChunkCatalogState.Enqueued).ToList();

            return new ChunkCatalogSnapshot(
                available.Count,
                enqueued.Count,
                available.Count == 0 ? null : now - available.Min(static entry => entry.Chunk.Metadata.CreatedUtc),
                enqueued.Count == 0 ? null : now - enqueued.Min(static entry => entry.Chunk.Metadata.CreatedUtc));
        }
    }

    private static long ToPriority(DateTimeOffset createdUtc) => createdUtc.UtcTicks;

    private sealed class Entry(StoredChunk chunk, ChunkCatalogState state)
    {
        public StoredChunk Chunk { get; } = chunk;
        public ChunkCatalogState State { get; set; } = state;
    }
}

internal readonly record struct ChunkCatalogSnapshot(
    int AvailableCount,
    int EnqueuedCount,
    TimeSpan? OldestAvailableAge,
    TimeSpan? OldestEnqueuedAge);

internal enum ChunkCatalogState
{
    Available = 0,
    Enqueued = 1,
    Terminal = 2
}
