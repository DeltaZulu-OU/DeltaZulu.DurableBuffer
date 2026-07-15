using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using DeltaZulu.DurableBuffer.Configuration;

namespace DeltaZulu.DurableBuffer.Chunks;

internal sealed class ChunkBuilder : IDisposable
{
    private readonly DurableBufferOptions _options;
    private ChunkId _chunkId;
    private DateTimeOffset _createdUtc;
    private IncrementalHash _hash;
    private long _payloadBytes;
    private int _recordCount;
    private MemoryStream _stream;

    public ChunkBuilder(DurableBufferOptions options)
    {
        _options = options;
        _stream = new MemoryStream(Math.Min((int)options.MaxChunkBytes, 1024 * 1024));
        _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        _chunkId = ChunkId.NewChunkId();
        _createdUtc = DateTimeOffset.UtcNow;
        WriteHeader();
    }

    public ChunkId ChunkId => _chunkId;
    public long CurrentBytes => _stream.Length;
    public bool IsEmpty => _recordCount == 0;
    public int RecordCount => _recordCount;

    public bool ShouldRotate =>
        _recordCount > 0 && (
            _recordCount >= _options.MaxChunkRecords ||
            _stream.Length + ChunkFormat.FooterSize >= _options.MaxChunkBytes ||
            (DateTimeOffset.UtcNow - _createdUtc) >= _options.MaxChunkAge);

    public void Append(ReadOnlyMemory<byte> serializedRecord)
    {
        var span = serializedRecord.Span;

        Span<byte> lengthPrefix = stackalloc byte[ChunkFormat.RecordLengthSize];
        BinaryPrimitives.WriteInt32LittleEndian(lengthPrefix, span.Length);

        _stream.Write(lengthPrefix);
        _hash.AppendData(lengthPrefix);

        _stream.Write(span);
        _hash.AppendData(span);

        _recordCount++;
        _payloadBytes += span.Length;
    }

    public void Dispose()
    {
        _hash.Dispose();
        _stream.Dispose();
    }

    public void Reset()
    {
        _hash.Dispose();
        _stream.Dispose();

        _stream = new MemoryStream(Math.Min((int)_options.MaxChunkBytes, 1024 * 1024));
        _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        _chunkId = ChunkId.NewChunkId();
        _createdUtc = DateTimeOffset.UtcNow;
        _recordCount = 0;
        _payloadBytes = 0;
        WriteHeader();
    }

    public (byte[] Data, ChunkMetadata Metadata) Seal()
    {
        Span<byte> footer = stackalloc byte[ChunkFormat.FooterSize];
        BinaryPrimitives.WriteInt32LittleEndian(footer, _recordCount);

        var hashBytes = _hash.GetHashAndReset();
        hashBytes.AsSpan().CopyTo(footer.Slice(ChunkFormat.FooterChecksumOffset));

        ChunkFormat.FooterMagic.CopyTo(footer.Slice(ChunkFormat.FooterMagicOffset));

        _stream.Write(footer);

        var data = _stream.ToArray();
        var checksum = "sha256:" + Convert.ToHexStringLower(hashBytes);

        var metadata = new ChunkMetadata {
            ChunkId = _chunkId.Value,
            CreatedUtc = _createdUtc,
            SealedUtc = DateTimeOffset.UtcNow,
            RecordCount = _recordCount,
            PayloadBytes = _payloadBytes,
            Checksum = checksum
        };

        return (data, metadata);
    }

    public bool WouldExceedLimit(int recordBytes) =>
                        _stream.Length + ChunkFormat.RecordLengthSize + recordBytes + ChunkFormat.FooterSize > _options.MaxChunkBytes;

    private void WriteHeader()
    {
        Span<byte> header = stackalloc byte[ChunkFormat.HeaderSize];
        header.Clear();

        ChunkFormat.Magic.CopyTo(header[ChunkFormat.MagicOffset..]);
        header[ChunkFormat.VersionOffset] = ChunkFormat.Version;

        Encoding.ASCII.GetBytes(_chunkId.Value, header[ChunkFormat.ChunkIdOffset..]);

        BinaryPrimitives.WriteInt64LittleEndian(
            header[ChunkFormat.CreatedUtcOffset..],
            _createdUtc.UtcTicks);

        _stream.Write(header);
    }
}
