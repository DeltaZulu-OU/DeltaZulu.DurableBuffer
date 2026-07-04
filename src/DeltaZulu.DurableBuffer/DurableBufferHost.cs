using System.Threading.Channels;
using DeltaZulu.DurableBuffer.Abstractions;
using DeltaZulu.DurableBuffer.Chunks;
using DeltaZulu.DurableBuffer.Configuration;
using DeltaZulu.DurableBuffer.Metrics;
using DeltaZulu.DurableBuffer.Recovery;
using DeltaZulu.DurableBuffer.Storage;
using Microsoft.Extensions.Logging;

namespace DeltaZulu.DurableBuffer;

public sealed class DurableBufferHost<T> : IAsyncDisposable
{
    private readonly DurableBuffer<T> _buffer;
    private readonly DurableBufferReader _reader;
    private readonly FileSystemRecoveryManager _recoveryManager;
    private readonly BufferEventBroadcaster _eventBroadcaster;
    private readonly BufferMetricsCounter _metrics;
    private readonly FileChunkStore _store;
    private readonly Channel<StoredChunk> _chunkChannel;
    private readonly DurableBufferOptions _options;
    private readonly ILogger? _logger;

    private Task? _rotationTask;
    private CancellationTokenSource? _cts;
    private bool _started;
    private bool _disposed;

    public DurableBufferHost(
        DurableBufferOptions options,
        IRecordSerializer<T> serializer,
        ILoggerFactory? loggerFactory = null)
    {
        _options = options;
        _logger = loggerFactory?.CreateLogger<DurableBufferHost<T>>();

        _eventBroadcaster = new BufferEventBroadcaster();
        _metrics = new BufferMetricsCounter();
        _metrics.UpdateDiskUsage(0, options.MaxDiskBytes);

        _chunkChannel = Channel.CreateBounded<StoredChunk>(
            new BoundedChannelOptions(1024) {
                SingleReader = false,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });

        _store = new FileChunkStore(
            options.StoragePath,
            loggerFactory?.CreateLogger<FileChunkStore>(),
            options.MaxDeadLetterBytes,
            options.MaxQuarantineBytes,
            onDeadLetterEvicted: evicted => {
                _metrics.AddDeadLetterBytes(-evicted.Metadata.PayloadBytes);
                _metrics.ChunkDeadLetterEvicted();
                _eventBroadcaster.Publish(BufferEvent.Create(
                    BufferEventType.BufferDeadLetterEvicted, evicted.Id.Value,
                    detail: "Dead-letter capacity exceeded; evicted oldest chunk", chunk: evicted));
            },
            onQuarantineEvicted: (path, bytes) => {
                _metrics.AddQuarantineBytes(-bytes);
                _metrics.ChunkQuarantineEvicted();
                _eventBroadcaster.Publish(BufferEvent.Create(
                    BufferEventType.BufferQuarantineEvicted,
                    detail: $"Quarantine capacity exceeded; evicted {Path.GetFileName(path)}"));
            });

        var backpressure = new BackpressureController(options);

        _buffer = new DurableBuffer<T>(
            serializer, _store, options, _metrics,
            _eventBroadcaster, backpressure, _chunkChannel.Writer);

        _reader = new DurableBufferReader(
            _chunkChannel.Reader, _chunkChannel.Writer,
            _store, _metrics, _eventBroadcaster,
            _buffer.SignalSpaceAvailable);

        _recoveryManager = new FileSystemRecoveryManager(
            _store, _chunkChannel.Writer, _metrics,
            _eventBroadcaster, options.StoragePath,
            loggerFactory?.CreateLogger<FileSystemRecoveryManager>());
    }

    public IDurableBufferWriter<T> Writer => _buffer;
    public IDurableBufferReader Reader => _reader;
    public IObservable<BufferEvent> Events => _eventBroadcaster;

    public async ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_started)
        {
            throw new InvalidOperationException("Buffer host is already started.");
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        _eventBroadcaster.Publish(BufferEvent.Create(BufferEventType.BufferStarted));

        await _recoveryManager.RecoverAsync(_cts.Token);

        var diskUsed = await _store.GetDiskBytesUsedAsync(_cts.Token);
        _metrics.UpdateDiskUsage(diskUsed, _options.MaxDiskBytes);

        var deadLetterBytesUsed = await _store.GetDeadLetterBytesUsedAsync(_cts.Token);
        _metrics.UpdateDeadLetterUsage(deadLetterBytesUsed, _options.MaxDeadLetterBytes);

        var quarantineBytesUsed = await _store.GetQuarantineBytesUsedAsync(_cts.Token);
        _metrics.UpdateQuarantineUsage(quarantineBytesUsed, _options.MaxQuarantineBytes);

        _rotationTask = Task.Run(() => RunRotationTimerAsync(_cts.Token), _cts.Token);

        _started = true;
        _logger?.LogInformation("Buffer host started. Storage: {Path}", _options.StoragePath);
    }

    public async ValueTask StopAsync(CancellationToken cancellationToken = default)
    {
        if (!_started || _disposed)
        {
            return;
        }

        _buffer.MarkStopping();
        _logger?.LogInformation("Buffer host stopping...");

        await _buffer.FlushAsync(cancellationToken);

        _chunkChannel.Writer.TryComplete();

        if (_cts is not null)
        {
            await _cts.CancelAsync();
        }

        if (_rotationTask is not null)
        {
            try { await _rotationTask.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken); }
            catch (TimeoutException) { }
            catch (OperationCanceledException) { }
        }

        _eventBroadcaster.Publish(BufferEvent.Create(BufferEventType.BufferStopped));
        _eventBroadcaster.Complete();

        _started = false;
        _logger?.LogInformation("Buffer host stopped.");
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        if (_started)
        {
            await StopAsync();
        }

        _buffer.Dispose();
        _cts?.Dispose();
        _disposed = true;
    }

    private async Task RunRotationTimerAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(500));
        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            try
            {
                await _buffer.RotateIfStaleAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error in rotation timer");
            }
        }
    }
}