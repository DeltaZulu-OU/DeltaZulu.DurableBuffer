# DeltaZulu.DurableBuffer

DeltaZulu.DurableBuffer is a .NET 10 library for durable, disk-backed buffering between a producer pipeline and an application-owned consumer. It provides crash-safe sealed chunks, bounded live disk and memory usage, configurable backpressure, startup recovery, metrics snapshots, observable lifecycle events, and bounded dead-letter and quarantine storage.

The library is intentionally small: it has no runtime dependencies beyond `Microsoft.Extensions.Logging.Abstractions` and stores format-agnostic byte records supplied by your serializer.

## Features

- **Durable local storage**: sealed chunks are written atomically to disk and survive process crashes.
- **Application-owned consumption**: records are accumulated in memory, sealed by record count, byte size, or age, and exposed through a `ChannelReader<StoredChunk>` for your consumer to complete, release, or dead-letter.
- **Backpressure controls**: choose whether a full live buffer blocks, rejects the newest record, or drops the oldest sealed chunk.
- **Consumer-directed retry and dead-lettering**: consumers can release chunks back to the sealed queue for retry or move failed chunks into bounded dead-letter storage.
- **Startup recovery**: valid sealed chunks are re-enqueued, incomplete active files are quarantined, legacy `dispatching/` files are migrated back to `sealed/`, and orphaned files are quarantined.
- **Metrics and events**: query `BufferSnapshot` and subscribe to `BufferEvent` notifications, including dead-letter and quarantine eviction events.
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

Configure storage and limits, start a host, run a consumer over the sealed chunk channel, write records, and stop the host to flush any open chunk.

```csharp
using DeltaZulu.DurableBuffer;
using DeltaZulu.DurableBuffer.Abstractions;
using DeltaZulu.DurableBuffer.Configuration;

var options = new DurableBufferOptions
{
    StoragePath = Path.Combine(AppContext.BaseDirectory, "buffer"),
    MaxDiskBytes = 512L * 1024 * 1024,
    MaxMemoryBytes = 32L * 1024 * 1024,
    MaxDeadLetterBytes = 64L * 1024 * 1024,
    MaxQuarantineBytes = 64L * 1024 * 1024,
    MaxChunkRecords = 1_000,
    MaxChunkBytes = 4L * 1024 * 1024,
    MaxChunkAge = TimeSpan.FromSeconds(5),
    FullPolicy = BufferFullPolicy.Block
};

await using var host = new DurableBufferHost<MyRecord>(
    options,
    new JsonRecordSerializer<MyRecord>());

await host.StartAsync();

var consumerTask = Task.Run(async () =>
{
    await foreach (var chunk in host.Reader.SealedChunks.ReadAllAsync())
    {
        try
        {
            // Read chunk.ChunkFilePath and forward it to your destination.
            await ForwardChunkAsync(chunk);
            await host.Reader.CompleteAsync(chunk);
        }
        catch (TransientForwardingException)
        {
            await host.Reader.ReleaseAsync(chunk);
        }
        catch (Exception ex)
        {
            await host.Reader.DeadLetterAsync(chunk, ex.Message);
        }
    }
});

var result = await host.Writer.WriteAsync(new MyRecord("hello"));
if (!result.IsAccepted)
{
    // Inspect result.Status for the reason.
}

var snapshot = host.Writer.GetSnapshot();

await host.StopAsync();
await consumerTask;
```

## Core API

### `DurableBufferHost<T>`

`DurableBufferHost<T>` wires together the write path, reader channel, file store, recovery manager, metrics, and event broadcaster.

- `StartAsync(...)`: performs recovery, starts age-based chunk rotation, and publishes startup events.
- `StopAsync(...)`: marks the writer as stopping, flushes the open chunk, completes the sealed-chunk channel, and publishes stopped events.
- `Writer`: exposes `IDurableBufferWriter<T>` for writes, flushes, and snapshots.
- `Reader`: exposes `IDurableBufferReader` for consuming sealed chunks and reporting outcomes.
- `Events`: exposes an `IObservable<BufferEvent>` stream.

### `IDurableBufferWriter<T>`

- `WriteAsync(T record, ...)`: serializes and appends a record, returning `BufferWriteResult`.
- `FlushAsync(...)`: seals and enqueues the currently open chunk, if any.
- `GetSnapshot()`: returns point-in-time state and counters.

### `IDurableBufferReader`

Use the reader from your own forwarding loop:

- `SealedChunks`: a `ChannelReader<StoredChunk>` that yields chunks ready for processing.
- `CompleteAsync(StoredChunk, ...)`: deletes a successfully processed chunk and frees live-buffer disk capacity.
- `ReleaseAsync(StoredChunk, ...)`: re-enqueues a chunk for application-managed retry.
- `DeadLetterAsync(StoredChunk, reason, ...)`: moves a failed chunk to bounded dead-letter storage and frees live-buffer disk capacity.

### Serialization

`JsonRecordSerializer<T>` serializes records with `System.Text.Json`. For other encodings, implement `IRecordSerializer<T>` and return the serialized bytes for each record.

## Configuration

| Option | Default | Description |
|---|---:|---|
| `StoragePath` | required | Root directory for active, sealed, dead-letter, and quarantine files. |
| `MaxDiskBytes` | 512 MiB | Maximum live sealed-chunk disk usage before the buffer is considered full. Dead-letter and quarantine bytes are tracked separately. |
| `MaxMemoryBytes` | 32 MiB | Maximum in-memory usage for open chunks. |
| `MaxDeadLetterBytes` | 64 MiB | Capacity for dead-letter storage. When exceeded, the oldest dead-lettered chunks are evicted. |
| `MaxQuarantineBytes` | 64 MiB | Capacity for quarantined files. When exceeded, the oldest quarantined files are evicted. |
| `MaxChunkRecords` | 1,000 | Record-count rotation threshold. |
| `MaxChunkBytes` | 4 MiB | Byte-size rotation threshold. |
| `MaxChunkAge` | 5 seconds | Age-based rotation threshold checked periodically. |
| `FullPolicy` | `Block` | Behavior when the live buffer is full. |

### Full-buffer policies

| Policy | Behavior |
|---|---|
| `Block` | Waits until a consumer completes or dead-letters chunks and frees live-buffer space. |
| `RejectNewest` | Rejects the incoming write immediately. |
| `DropOldest` | Deletes the oldest sealed chunk and accepts the incoming write when possible. |

## Storage model

The storage root contains state directories managed by the file store:

```text
active/       Temporary open chunk files; incomplete files are quarantined on recovery.
sealed/       Durable chunks waiting for application consumption.
deadletter/   Chunks explicitly dead-lettered by the consumer; bounded independently.
quarantine/   Incomplete, invalid, or orphaned files preserved for inspection; bounded independently.
```

Completed chunks use the `.chunk` payload file and a companion `.meta.json` metadata file. The live-disk backpressure budget counts `sealed/`; dead-letter and quarantine directories have independent budgets and evict oldest entries when their caps are exceeded.

## Recovery behavior

On startup, the recovery manager:

1. Quarantines incomplete active files.
2. Migrates files from the legacy `dispatching/` directory back to `sealed/`.
3. Validates and re-enqueues sealed chunks.
4. Quarantines invalid chunks or orphaned metadata/payload files.
5. Updates live disk, dead-letter, and quarantine usage metrics after recovery.

## Metrics and events

Use `GetSnapshot()` for counters and current state:

```csharp
var snapshot = host.Writer.GetSnapshot();
Console.WriteLine($"State: {snapshot.State}, disk: {snapshot.DiskBytesUsed}/{snapshot.DiskBytesLimit}");
Console.WriteLine($"Dead letter: {snapshot.DeadLetterBytesUsed}/{snapshot.DeadLetterBytesLimit}");
```

Subscribe to events to observe lifecycle, pressure, chunk processing, dead-lettering, eviction, and recovery activity:

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
