# DeltaZulu.DurableBuffer Architecture

DeltaZulu.DurableBuffer is a local durable buffering library for .NET 10. It sits between the DeltaZulu collector pipeline and the forwarder, providing crash-safe, disk-backed buffering with backpressure, retry, and dead-lettering.

## Design principles

1. **Single-buffer primitive.** The library manages one buffer with one dispatch path. Multi-buffer patterns (hot path + dead-letter overflow) are composed by the consumer instantiating two `DurableBufferHost` instances with different configurations.
2. **Crash safety over throughput.** Data sealed to disk survives process crashes. The open chunk (in-memory) is lost on crash and quarantined on recovery.
3. **Format-agnostic payloads.** The binary chunk format stores length-prefixed byte records. The caller provides serialized bytes via `IRecordSerializer<T>`.
4. **No external dependencies** beyond `Microsoft.Extensions.Logging.Abstractions`.

## Components

```text
┌─────────────────────────────────────────────────────────┐
│                   DurableBufferHost<T>                │
│  ┌─────────────────┐  ┌──────────────┐  ┌───────────┐  │
│  │ DurableBuffer  │  │DispatchWorker│  │ Recovery  │  │
│  │  (write path)    │  │ (send path)  │  │ Manager   │  │
│  └────────┬─────────┘  └──────┬───────┘  └─────┬─────┘  │
│           │                   │                │         │
│  ┌────────▼─────────┐  ┌─────▼────────┐       │         │
│  │  ChunkBuilder    │  │ RetryScheduler│       │         │
│  │  (in-memory)     │  │ + PriorityQ   │       │         │
│  └────────┬─────────┘  └──────────────┘       │         │
│           │                                    │         │
│  ┌────────▼────────────────────────────────────▼──────┐  │
│  │              FileChunkStore (disk I/O)             │  │
│  │   active/ → sealed/ → dispatching/ → (delete)     │  │
│  │                 ↘ deadletter/   ↗ quarantine/      │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  ┌───────────────────┐  ┌──────────────────────────┐    │
│  │BackpressureControl│  │BufferMetrics + Events    │    │
│  └───────────────────┘  └──────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

## Data flow

1. **Write:** Caller calls `WriteAsync(T)`. The record is serialized and appended to the in-memory `ChunkBuilder`.
2. **Rotation:** When the chunk reaches record count, byte size, or age limits, it is sealed (binary format with SHA256 checksum) and written atomically to `sealed/`.
3. **Dispatch:** The `DispatchWorker` reads sealed chunks from a `Channel<StoredChunk>`, moves each to `dispatching/`, and calls `IChunkSender.SendAsync`.
4. **ACK:** On success, the chunk files are deleted. On transient failure, the chunk moves back to `sealed/` with an incremented attempt count and is enqueued in a `PriorityQueue` for retry.
5. **Dead-letter:** After retry exhaustion (or on permanent failure), the chunk moves to `deadletter/`.
6. **Recovery:** On startup, `FileSystemRecoveryManager` quarantines incomplete active files, validates and re-enqueues dispatching/sealed chunks, and quarantines orphans.

## Binary chunk format

```
Offset  Size  Content
0       4     Magic "DZBC" (0x44 0x5A 0x42 0x43)
4       4     Format version (1)
8       4     Record count (uint32 LE)
12      4     Reserved
16      32    Reserved (future header fields)
--- records ---
N       4     Record length (uint32 LE)
N+4     L     Record payload (L bytes)
--- footer ---
F       4     Footer magic "DZFE" (0x44 0x5A 0x46 0x45)
F+4     4     Payload byte count (uint32 LE)
F+8     32    SHA-256 hash of header + records
```

Header size: 48 bytes. Footer size: 40 bytes.

## Backpressure states

| State | Condition | Accept writes? |
|---|---|---|
| Healthy | Under limits, no retries pending | Yes |
| Degraded | Under limits, retry queue non-empty | Yes |
| Pressured | Disk or memory usage > 85% of limit | Yes |
| Full | Disk or memory at/over limit | No (policy-dependent) |

When full, the configured `BufferFullPolicy` determines behavior:
- **Block:** Waits on a semaphore until space is freed.
- **RejectNewest:** Returns `RejectedBufferFull` immediately.
- **DropOldest:** Deletes the oldest sealed chunk to make room.

## Retry strategy

Exponential backoff with ±25% jitter:

```
delay = min(maxDelay, baseDelay × 2^attempt) × (1 ± 0.25)
```

Default: base 1s, max 5min, 10 attempts. After exhaustion, the `RetryExhaustedPolicy` applies: `DeadLetter` (default) or `Discard`.

## File state transitions

```
active/*.tmp  →  (quarantined on recovery)
sealed/*.chunk + *.meta.json  →  dispatching/  →  (deleted on success)
                                              →  sealed/ (on transient failure, retry)
                                              →  deadletter/ (on permanent failure or retry exhaustion)
```

All state transitions use atomic `File.Move`. Temp files use `.tmp` suffix and are renamed on completion.

## Security

- Symlink protection on all file operations (`EnsureNotSymlink`).
- Unix file mode hardening (owner-only rwx) on non-Windows.
- Atomic writes via temp file + rename.

## Concurrency model

- `SemaphoreSlim(1,1)` serializes write operations on `ChunkBuilder`.
- `System.Threading.Channels` (bounded, single-reader) connects the write path to the dispatch worker.
- All metrics counters use `Interlocked` operations.
- Event broadcasting uses `ImmutableArray<T>` with `ImmutableInterlocked.InterlockedCompareExchange`.
- `PeriodicTimer` checks for stale chunks every 500ms.

## Consumer composition: two-buffer pattern

The library stays a single-buffer primitive. To implement a dead-letter overflow buffer:

```csharp
var bufferA = new DurableBufferHost<T>(optionsA, serializer, primarySender);
var bufferB = new DurableBufferHost<T>(optionsB, serializer, fallbackSender);

bufferA.Events
    .Where(e => e.EventType == BufferEventType.BufferChunkDeadLettered)
    .Subscribe(async evt =>
    {
        var data = await File.ReadAllBytesAsync(evt.Chunk!.ChunkFilePath);
        var records = ChunkFormat.ReadRecords(data);
        foreach (var record in records)
            await bufferB.Buffer.WriteAsync(deserialize(record));
    });
```

Each host has independent storage, retry configuration, and sender.
