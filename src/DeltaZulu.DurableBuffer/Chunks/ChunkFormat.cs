using System.Buffers.Binary;
using System.Security.Cryptography;

namespace DeltaZulu.DurableBuffer.Chunks;

public static class ChunkFormat
{
    public static ReadOnlySpan<byte> Magic => "DZBC"u8;
    public const byte Version = 0x01;
    public const int HeaderSize = 48;

    public static ReadOnlySpan<byte> FooterMagic => "DZFE"u8;
    public const int FooterSize = 40;
    public const int RecordLengthSize = 4;

    public const int MagicOffset = 0;
    public const int VersionOffset = 4;
    public const int ReservedOffset = 5;
    public const int ChunkIdOffset = 8;
    public const int CreatedUtcOffset = 40;

    public const int FooterRecordCountOffset = 0;
    public const int FooterChecksumOffset = 4;
    public const int FooterMagicOffset = 36;

    public static IReadOnlyList<ReadOnlyMemory<byte>> ReadRecords(ReadOnlyMemory<byte> chunkData)
    {
        var span = chunkData.Span;
        if (span.Length < HeaderSize + FooterSize)
        {
            throw new InvalidDataException("Chunk data is too small to contain header and footer.");
        }

        if (!span[..4].SequenceEqual(Magic))
        {
            throw new InvalidDataException("Invalid chunk magic bytes.");
        }

        var records = new List<ReadOnlyMemory<byte>>();
        var offset = HeaderSize;
        var footerStart = span.Length - FooterSize;

        while (offset + RecordLengthSize <= footerStart)
        {
            var length = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(offset, RecordLengthSize));
            offset += RecordLengthSize;

            if (offset + length > footerStart)
            {
                throw new InvalidDataException("Record extends past footer boundary.");
            }

            records.Add(chunkData.Slice(offset, length));
            offset += length;
        }

        return records;
    }

    public static bool ValidateChecksum(ReadOnlySpan<byte> chunkData)
    {
        if (chunkData.Length < HeaderSize + FooterSize)
        {
            return false;
        }

        if (!chunkData[^4..].SequenceEqual(FooterMagic))
        {
            return false;
        }

        var footerStart = chunkData.Length - FooterSize;
        var recordRegion = chunkData[HeaderSize..footerStart];

        Span<byte> computed = stackalloc byte[32];
        SHA256.HashData(recordRegion, computed);

        var stored = chunkData.Slice(footerStart + FooterChecksumOffset, 32);
        return computed.SequenceEqual(stored);
    }
}