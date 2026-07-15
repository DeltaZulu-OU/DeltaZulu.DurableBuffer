using DeltaZulu.DurableBuffer.Abstractions;
using DeltaZulu.DurableBuffer.Configuration;

namespace DeltaZulu.DurableBuffer;

internal sealed class BackpressureController
{
    private const double PressureThreshold = 0.85;
    private readonly DurableBufferOptions _options;

    public BackpressureController(DurableBufferOptions options)
    {
        _options = options;
    }

    public (BufferState State, bool ShouldAccept) Evaluate(
        long diskBytesUsed,
        long memoryBytesUsed)
    {
        if (diskBytesUsed >= _options.MaxDiskBytes || memoryBytesUsed >= _options.MaxMemoryBytes)
        {
            return (BufferState.Full, false);
        }

        var diskRatio = _options.MaxDiskBytes > 0
            ? (double)diskBytesUsed / _options.MaxDiskBytes
            : 0;
        var memRatio = _options.MaxMemoryBytes > 0
            ? (double)memoryBytesUsed / _options.MaxMemoryBytes
            : 0;

        if (diskRatio > PressureThreshold || memRatio > PressureThreshold)
        {
            return (BufferState.Pressured, true);
        }

        return (BufferState.Healthy, true);
    }
}
