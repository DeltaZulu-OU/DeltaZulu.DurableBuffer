# DeltaZulu.DurableBuffer Architecture

DeltaZulu.DurableBuffer is a local durable buffering library for .NET 10. It sits between a producer pipeline and an application-owned forwarding loop, providing crash-safe, disk-backed buffering with backpressure, recovery, consumer-directed retry, and bounded dead-lettering.

## Design principles

1. **Single-buffer primitive.** The library manages one live sealed-chunk queue. Multi-buffer patterns (hot path + dead-letter overflow) are composed by the consumer instantiating multiple `DurableBufferHost` instances with different configurations.
2. **Crash safety over throughput.** Data sealed to disk survives process crashes. The open in-memory chunk is not durable until rotation; incomplete active files are quarantined on recovery.
3. **Application-owned dispatch.** The library does not own network forwarding or retry policy. It exposes sealed chunks via `IDurableBufferReader.SealedChunks`, and consumers report success, retry, or dead-letter outcomes.
4. **Format-agnostic payloads.** The binary chunk format stores length-prefixed byte records. The caller provides serialized bytes via `IRecordSerializer<T>`.
5. **No external dependencies** beyond `Microsoft.Extensions.Logging.Abstractions`.

## Components

```text
┌─────────────────────────────────────────────────────────┐
│                   DurableBufferHost<T>                  │
│  ┌─────────────────┐  ┌──────────────┐  ┌───────────┐  │
│  │ DurableBuffer   │  │DurableBuffer │  │ Recovery  │  │
│  │  (write path)   │  │Reader        │  │ Manager   │  │
│  └────────┬────────┘  └──────┬───────┘  └─────┬─────┘  │
│           │                  │                │         │
│  ┌────────▼─────────┐  ┌─────▼────────┐       │         │
│  │  ChunkBuilder    │  │ Channel<     │       │         │
│  │  (in-memory)     │  │ StoredChunk> │       │         │
│  └────────┬─────────┘  └──────────────┘       │         │
│           │                                    │         │
│  ┌────────▼────────────────────────────────────▼──────┐  │
│  │              FileChunkStore (disk I/O)             │  │
│  │              active/ → sealed/ → (delete)          │  │
│  │                    ↘ deadletter/   quarantine/     │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  ┌───────────────────┐  ┌──────────────────────────┐    │
│  │BackpressureControl│  │BufferMetrics + Events    │    │
│  └───────────────────┘  └──────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

## Data flow

1. **Write:** Caller calls `Writer.WriteAsync(T)`. The record is serialized and appended to the in-memory `ChunkBuilder`.
2. **Rotation:** When the chunk reaches record count, byte size, or age limits, it is sealed (binary format with SHA-256 checksum) and written atomically to `sealed/`.
3. **Enqueue:** The sealed `StoredChunk` is published to the internal `Channel<StoredChunk>` and exposed through `Reader.SealedChunks`.
4. **Consume:** Application code reads chunks from the channel and forwards them to its destination.
5. **Complete:** On success, the consumer calls `Reader.CompleteAsync(chunk)`, which deletes the chunk files and frees live-buffer disk capacity.
6. **Retry:** On a retryable failure, the consumer calls `Reader.ReleaseAsync(chunk)`, which re-enqueues the same chunk for another consumer attempt.
7. **Dead-letter:** On a terminal failure, the consumer calls `Reader.DeadLetterAsync(chunk, reason)`, which moves the chunk into bounded `deadletter/` storage and frees live-buffer disk capacity.
8. **Recovery:** On startup, `FileSystemRecoveryManager` quarantines incomplete active files, migrates legacy dispatching files, validates and re-enqueues sealed chunks, and quarantines orphans.

## Binary chunk format

```text
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
| Healthy | Live sealed disk usage and open-chunk memory are under pressure thresholds | Yes |
| Pressured | Live disk or memory usage is over 85% of its limit | Yes |
| Full | Live disk or memory usage is at or over its limit | No (policy-dependent) |

When full, the configured `BufferFullPolicy` determines behavior:

- **Block:** Waits until a consumer completes or dead-letters chunks and signals that live-buffer space is available.
- **RejectNewest:** Returns `RejectedBufferFull` immediately.
- **DropOldest:** Deletes the oldest sealed chunk to make room.

Dead-letter and quarantine files do not count against `MaxDiskBytes`; they have independent ring-buffer caps (`MaxDeadLetterBytes` and `MaxQuarantineBytes`).

## Consumer retry strategy

Retry policy is intentionally owned by the consumer. The buffer only provides primitives:

- `ReleaseAsync` immediately makes the chunk available on `SealedChunks` again.
- Consumers that need backoff can delay before calling `ReleaseAsync`, or maintain their own scheduler and call `ReleaseAsync` when the chunk should be retried.
- Consumers that exhaust their retry budget should call `DeadLetterAsync` with a reason for observability.

## File state transitions

```text
active/*.tmp  →  (quarantined on recovery)
sealed/*.chunk + *.meta.json  →  (deleted on CompleteAsync)
                              →  sealed/ (ReleaseAsync re-enqueues the existing files)
                              →  deadletter/ (DeadLetterAsync)
legacy dispatching/*          →  sealed/ (constructor migration)
invalid or orphaned files     →  quarantine/
```

Sealed chunks are written via temp files and renamed into place on completion. Dead-letter and quarantine storage are bounded; when a cap is exceeded, the oldest entries are evicted and eviction events are published.

## Security

- Symlink protection on file operations (`EnsureNotSymlink`).
- Unix file mode hardening (owner-only rwx) on non-Windows.
- Atomic chunk writes via temp file + rename.

## Concurrency model

- `SemaphoreSlim(1,1)` serializes write operations on `ChunkBuilder`.
- `System.Threading.Channels` connects the write path to application consumers.
- `DeltaZulu.DurableBuffer.Rx` exposes durable-buffer-owned reactive contracts for demand-aware chunk dispatch and lifecycle-safe event streams.
- The sealed chunk channel is bounded and supports multiple readers and writers.
- All metrics counters use `Interlocked` operations.
- Event broadcasting uses `ImmutableArray<T>` with `ImmutableInterlocked.InterlockedCompareExchange`.
- `PeriodicTimer` checks for stale chunks every 500 ms.

## Primitive responsibility map

| Boundary | Primitive | Reason |
|---|---|---|
| Durable chunk state | Buffer/file-store state under controlled mutation | Authoritative transitions remain deterministic. |
| Chunk dispatch implementation | `Channel<StoredChunk>` | Bounded async producer/consumer handoff. |
| Chunk dispatch contract | `DeltaZulu.DurableBuffer.Rx` (`IRxPublisher<T>`, `IRxSubscriber<T>`, `IRxSubscription`) | Exposes demand, cancellation, and completion without leaking channel internals. |
| Buffer events contract | `DeltaZulu.DurableBuffer.Rx` (`IRxEventStream<TEvent>`, `IRxEventSink<TEvent>`) | Stable observability contracts without external reactive dependencies. |
| Byte stream parsing | `PipeReader` / `PipeWriter` (at transport boundaries) | Handles partial frames and pooled buffer parsing efficiently. |
| Background work | Explicit `Task` loops | Clear lifecycle and cancellation behavior. |

## Reactive non-goals

- No Rx.NET dependency in DurableBuffer core.
- No R3 dependency in DurableBuffer core.
- No Reactive Streams compliance claim.
- No full operator/scheduler framework in core.
- No unbounded channels for durable chunk dispatch.
- No broadcast pub-sub semantics for durable chunk delivery. `RxChunks` is demand-aware queue dispatch over the sealed-chunk queue: a delivered chunk is read from the queue for one consumer attempt, not replayed to every active subscriber.

## Durable pub-sub wrapper acceptance criteria

DurableBuffer remains a single-buffer queue primitive. A separate DurablePubSub wrapper may compose one or more DurableBuffer instances to provide broadcast-style pub-sub behavior, but that wrapper must satisfy these acceptance criteria rather than relying on DurableBuffer chunk dispatch to provide them:

- DurableBuffer docs explicitly say Rx chunk delivery is queue dispatch, not broadcast pub-sub.
- DurablePubSub has named durable subscribers.
- Publish generates a stable `eventId` or `publishId` before subscriber writes.
- Required subscriber buffers must accept before `Publish` succeeds.
- Optional subscriber failure never blocks required subscriber delivery.
- Each subscriber has independent buffer storage, ACK, retry, DLQ, and metrics.
- Cross-branch correlation uses `eventId` or `publishId`, not branch-local `chunkId`.
- Integration tests cover partial publish failure.
- Integration tests cover restart after some subscriber writes succeeded.
- Metrics distinguish required publish failure from optional sample drop.

## Consumer composition: two-buffer pattern

The library stays a single-buffer primitive. To implement a dead-letter overflow buffer, subscribe to dead-letter events or consume files from `deadletter/` and write records to a second host:

```csharp
var bufferA = new DurableBufferHost<T>(optionsA, serializer);
var bufferB = new DurableBufferHost<T>(optionsB, serializer);

bufferA.Events
    .Where(e => e.EventType == BufferEventType.BufferChunkDeadLettered && e.Chunk is not null)
    .Subscribe(async evt =>
    {
        var data = await File.ReadAllBytesAsync(evt.Chunk!.ChunkFilePath);
        var records = ChunkFormat.ReadRecords(data);
        foreach (var record in records)
            await bufferB.Writer.WriteAsync(deserialize(record));
    });
```

Each host has independent storage, backpressure configuration, dead-letter capacity, quarantine capacity, and consumer logic.
