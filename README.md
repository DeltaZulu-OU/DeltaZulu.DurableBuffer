# DeltaZulu.DurableBuffer

DeltaZulu.DurableBuffer is a .NET 10 library for durable, disk-backed buffering between a producer pipeline and a chunk forwarder. It provides crash-safe sealed chunks, bounded disk and memory usage, configurable backpressure, exponential retry, recovery, metrics snapshots, observable lifecycle events, and dead-letter handling.

The library is intentionally small: it has no runtime dependencies beyond `Microsoft.Extensions.Logging.Abstractions` and stores format-agnostic byte records supplied by your serializer.

## Features

- **Durable local storage**: sealed chunks are written atomically to disk and survive process crashes.
- **Chunked dispatch**: records are accumulated in memory, sealed by record count, byte size, or age, then sent through an `IChunkSender`.
- **Backpressure controls**: choose whether a full buffer blocks, rejects the newest record, or drops the oldest sealed chunk.
- **Retry and dead-lettering**: transient failures use exponential backoff with jitter; permanent or exhausted chunks can be dead-lettered or discarded.
- **Startup recovery**: interrupted active files are quarantined, valid sealed/dispatching chunks are re-enqueued, and orphaned files are quarantined.
- **Metrics and events**: query `BufferSnapshot` and subscribe to `BufferEvent` notifications.
- **Format-agnostic records**: provide any `IRecordSerializer<T>` implementation; a JSON serializer is included.

## Requirements

- .NET 10 SDK or later.
- A writable local directory for buffer storage.

## Repository layout

```text
src/DeltaZulu.DurableBuffer/          Library source
tests/DeltaZulu.DurableBuffer.Tests/  MSTest test suite
docs/DURABLE_BUFFER_ARCHITECTURE.md   Architecture and file-format notes
```

## Installation

The package metadata identifies the library as `DeltaZulu.DurableBuffer`. Until a package is published in your environment, reference the project directly:

```xml
<ItemGroup>
  <ProjectReference Include="../DeltaZulu.DurableBuffer/src/DeltaZulu.DurableBuffer/DeltaZulu.DurableBuffer.csproj" />
</ItemGroup>
```

If consuming from a package feed, install the package with your preferred .NET tooling:

```bash
dotnet add package DeltaZulu.DurableBuffer
```

## Quick start

Create a sender, configure storage and limits, start a host, write records, and stop the host to flush any open chunk.

```csharp
using DeltaZulu.DurableBuffer;
using DeltaZulu.DurableBuffer.Chunks;
using DeltaZulu.DurableBuffer.Configuration;
using DeltaZulu.DurableBuffer.Dispatch;

sealed class HttpChunkSender : IChunkSender
{
    public async ValueTask<ChunkSendResult> SendAsync(
        StoredChunk chunk,
        CancellationToken cancellationToken = default)
    {
        // Read chunk.ChunkFilePath and forward it to your destination.
        // Return TransientFailure for retryable failures and PermanentFailure
        // for chunks that should not be retried.
        await Task.CompletedTask;
        return new ChunkSendResult(ChunkSendStatus.Success);
    }
}

var options = new DurableBufferOptions
{
    StoragePath = Path.Combine(AppContext.BaseDirectory, "buffer"),
    MaxDiskBytes = 512L * 1024 * 1024,
    MaxMemoryBytes = 32L * 1024 * 1024,
    MaxChunkRecords = 1_000,
    MaxChunkBytes = 4L * 1024 * 1024,
    MaxChunkAge = TimeSpan.FromSeconds(5),
    FullPolicy = BufferFullPolicy.Block,
    RetryExhaustedPolicy = RetryExhaustedPolicy.DeadLetter,
    MaxRetryAttempts = 10,
    RetryBaseDelay = TimeSpan.FromSeconds(1),
    RetryMaxDelay = TimeSpan.FromMinutes(5)
};

await using var host = new DurableBufferHost<MyRecord>(
    options,
    new JsonRecordSerializer<MyRecord>(),
    new HttpChunkSender());

await host.StartAsync();

var result = await host.Buffer.WriteAsync(new MyRecord("hello"));
if (!result.IsAccepted)
{
    // Inspect result.Status for the reason.
}

var snapshot = host.Buffer.GetSnapshot();

await host.StopAsync();
```

## Core API

### `DurableBufferHost<T>`

`DurableBufferHost<T>` wires together the write path, dispatch worker, file store, retry scheduler, recovery manager, metrics, and event broadcaster.

- `StartAsync(...)`: starts dispatching, performs recovery, and begins age-based chunk rotation.
- `StopAsync(...)`: marks the buffer as stopping, flushes the open chunk, drains dispatch where possible, and completes events.
- `Buffer`: exposes `IDurableBuffer<T>` for writes, flushes, and snapshots.
- `Events`: exposes an `IObservable<BufferEvent>` stream.

### `IDurableBuffer<T>`

- `WriteAsync(T record, ...)`: serializes and appends a record, returning `BufferWriteResult`.
- `FlushAsync(...)`: seals and enqueues the currently open chunk, if any.
- `GetSnapshot()`: returns point-in-time state and counters.

### `IChunkSender`

Implement `IChunkSender` to deliver sealed chunks to your destination. Return:

- `ChunkSendStatus.Success` when the chunk is delivered and can be deleted.
- `ChunkSendStatus.TransientFailure` when the chunk should be retried.
- `ChunkSendStatus.PermanentFailure` when the chunk should be dead-lettered or discarded according to policy.

### Serialization

`JsonRecordSerializer<T>` serializes records with `System.Text.Json`. For other encodings, implement `IRecordSerializer<T>` and return the serialized bytes for each record.

## Configuration

| Option | Default | Description |
|---|---:|---|
| `StoragePath` | required | Root directory for active, sealed, dispatching, dead-letter, and quarantine files. |
| `MaxDiskBytes` | 512 MiB | Maximum disk usage before the buffer is considered full. |
| `MaxMemoryBytes` | 32 MiB | Maximum in-memory usage for open chunks. |
| `MaxChunkRecords` | 1,000 | Record-count rotation threshold. |
| `MaxChunkBytes` | 4 MiB | Byte-size rotation threshold. |
| `MaxChunkAge` | 5 seconds | Age-based rotation threshold checked periodically. |
| `FullPolicy` | `Block` | Behavior when the buffer is full. |
| `RetryExhaustedPolicy` | `DeadLetter` | Behavior after retry attempts are exhausted. |
| `MaxRetryAttempts` | 10 | Retry attempt limit for transient failures. |
| `RetryBaseDelay` | 1 second | Initial exponential backoff delay. |
| `RetryMaxDelay` | 5 minutes | Maximum exponential backoff delay. |

### Full-buffer policies

| Policy | Behavior |
|---|---|
| `Block` | Waits until space is available. |
| `RejectNewest` | Rejects the incoming write immediately. |
| `DropOldest` | Deletes the oldest sealed chunk and accepts the incoming write when possible. |

## Storage model

The storage root contains state directories managed by the file store:

```text
active/       Temporary open chunk files; incomplete files are quarantined on recovery.
sealed/       Durable chunks waiting for dispatch.
dispatching/  Chunks currently being sent.
deadletter/   Chunks that permanently failed or exhausted retry attempts.
quarantine/   Incomplete, invalid, or orphaned files preserved for inspection.
```

State transitions use atomic file moves. Completed chunks use the `.chunk` payload file and a companion `.meta.json` metadata file.

## Recovery behavior

On startup, the recovery manager:

1. Quarantines incomplete active files.
2. Validates and re-enqueues sealed and dispatching chunks.
3. Quarantines invalid chunks or orphaned metadata/payload files.
4. Updates disk-usage metrics after recovery.

## Metrics and events

Use `GetSnapshot()` for counters and current state:

```csharp
var snapshot = host.Buffer.GetSnapshot();
Console.WriteLine($"State: {snapshot.State}, disk: {snapshot.DiskBytesUsed}/{snapshot.DiskBytesLimit}");
```

Subscribe to events to observe lifecycle, pressure, dispatch, retry, dead-letter, and recovery activity:

```csharp
using DeltaZulu.DurableBuffer.Metrics;

sealed class ConsoleBufferEventObserver : IObserver<BufferEvent>
{
    public void OnNext(BufferEvent evt) =>
        Console.WriteLine($"{evt.TimestampUtc:u} {evt.EventType} {evt.ChunkId}");

    public void OnError(Exception error) => Console.Error.WriteLine(error);

    public void OnCompleted() => Console.WriteLine("Buffer event stream completed.");
}

IDisposable subscription = host.Events.Subscribe(new ConsoleBufferEventObserver());
```

## Chunk format

Chunks use a binary format with:

- A fixed header containing magic bytes, format version, and record count.
- Length-prefixed record payloads.
- A footer containing footer magic, payload byte count, and a SHA-256 hash of the header and records.

See [`docs/DURABLE_BUFFER_ARCHITECTURE.md`](docs/DURABLE_BUFFER_ARCHITECTURE.md) for the detailed layout and design notes.

## Development

Restore, build, and test with the .NET SDK:

```bash
dotnet restore DeltaZulu.DurableBuffer.slnx
dotnet build DeltaZulu.DurableBuffer.slnx
dotnet test DeltaZulu.DurableBuffer.slnx
```

## License

This repository is licensed under the GNU Affero General Public License v3.0. See [`LICENSE.txt`](LICENSE.txt).
