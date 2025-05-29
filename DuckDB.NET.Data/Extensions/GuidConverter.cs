using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Extensions;

internal static class GuidConverter
{
#if !NET6_0_OR_GREATER
    private const string GuidFormat = "D";
    private static readonly char[] HexDigits = "0123456789abcdef".ToCharArray();

    //Ported from duckdb source code UUID::ToString
    //https://github.com/duckdb/duckdb/blob/9c91b3a329073ea1767b0aaff94b51da98dd03e2/src/common/types/uuid.cpp#L56
    public static Guid ConvertToGuid(this DuckDBHugeInt input)
    {
        Span<char> buffer = stackalloc char[36];
        var num = input.Upper ^ long.MinValue;
        var position = 0;
        
        ByteToHex(buffer, ref position, (ulong)((num >> 56) & 0xFF));
        ByteToHex(buffer, ref position, (ulong)((num >> 48) & 0xFF));
        ByteToHex(buffer, ref position, (ulong)((num >> 40) & 0xFF));
        ByteToHex(buffer, ref position, (ulong)((num >> 32) & 0xFF));
        
        buffer[position++] = '-';
        
        ByteToHex(buffer, ref position, (ulong)((num >> 24) & 0xFF));
        ByteToHex(buffer, ref position, (ulong)((num >> 16) & 0xFF));
        
        buffer[position++] = '-';
        
        ByteToHex(buffer, ref position, (ulong)((num >> 8) & 0xFF));
        ByteToHex(buffer, ref position, (ulong)(num & 0xFF));
        
        buffer[position++] = '-';
        
        ByteToHex(buffer, ref position, (input.Lower >> 56) & 0xFF);
        ByteToHex(buffer, ref position, (input.Lower >> 48) & 0xFF);
        
        buffer[position++] = '-';
        
        ByteToHex(buffer, ref position, (input.Lower >> 40) & 0xFF);
        ByteToHex(buffer, ref position, (input.Lower >> 32) & 0xFF);
        ByteToHex(buffer, ref position, (input.Lower >> 24) & 0xFF);
        ByteToHex(buffer, ref position, (input.Lower >> 16) & 0xFF);
        ByteToHex(buffer, ref position, (input.Lower >> 8) & 0xFF);
        ByteToHex(buffer, ref position, input.Lower & 0xFF);

        return Guid.ParseExact(new string(buffer.ToArray()), GuidFormat);
        
        static void ByteToHex(Span<char> buffer, ref int position, ulong value)
        {
            buffer[position++] = HexDigits[(value >> 4) & 0xF];
            buffer[position++] = HexDigits[value & 0xF];
        }
    }
#else
    public static Guid ConvertToGuid(this DuckDBHugeInt input)
    {
        var bytes = ArrayPool<byte>.Shared.Rent(32);
        try
        {
            // Reverse the bit flip on the upper 64 bits
            long upper = input.Upper ^ ((long)1 << 63);

            // Write upper 64 bits (bytes 0-7)
            BitConverter.TryWriteBytes(bytes.AsSpan(16), upper);

            // Write lower 64 bits (bytes 8-15)
            BitConverter.TryWriteBytes(bytes.AsSpan(16 + 8), input.Lower);

            // Reconstruct the Guid bytes (reverse the original byte reordering)
            ReorderBytesForGuid();

            // Create Guid from the first 16 bytes
            return new Guid(bytes.AsSpan(0, 16));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bytes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void ReorderBytesForGuid()
        {
            // First 4 bytes (little-endian)
            bytes[6] = bytes[16 + 0];
            bytes[7] = bytes[16 + 1];
            bytes[4] = bytes[16 + 2];
            bytes[5] = bytes[16 + 3];

            // Next 4 bytes (little-endian)
            bytes[0] = bytes[16 + 4];
            bytes[1] = bytes[16 + 5];
            bytes[2] = bytes[16 + 6];
            bytes[3] = bytes[16 + 7];

            // Last 8 bytes (big-endian)
            bytes[15] = bytes[16 + 8];
            bytes[14] = bytes[16 + 9];
            bytes[13] = bytes[16 + 10];
            bytes[12] = bytes[16 + 11];
            bytes[11] = bytes[16 + 12];
            bytes[10] = bytes[16 + 13];
            bytes[9] = bytes[16 + 14];
            bytes[8] = bytes[16 + 15];
        }
    }
#endif

    //https://github.com/duckdb/duckdb/blob/9c91b3a329073ea1767b0aaff94b51da98dd03e2/src/common/types/uuid.cpp#L6
    public static DuckDBHugeInt ToHugeInt(this Guid guid)
    {
        var bytes = ArrayPool<byte>.Shared.Rent(32);

        try
        {
#if NET6_0_OR_GREATER
            guid.TryWriteBytes(bytes);
#else
            Buffer.BlockCopy(guid.ToByteArray(), 0, bytes, 0, 16);
#endif
            bytes[16 + 0] = bytes[6]; // First 4 bytes (little-endian)
            bytes[16 + 1] = bytes[7];
            bytes[16 + 2] = bytes[4];
            bytes[16 + 3] = bytes[5];
            bytes[16 + 4] = bytes[0]; // Next 4 bytes (little-endian)
            bytes[16 + 5] = bytes[1];
            bytes[16 + 6] = bytes[2];
            bytes[16 + 7] = bytes[3];

            bytes[16 + 8] = bytes[15]; // Big endian 
            bytes[16 + 9] = bytes[14];
            bytes[16 + 10] = bytes[13];
            bytes[16 + 11] = bytes[12];
            bytes[16 + 12] = bytes[11];
            bytes[16 + 13] = bytes[10];
            bytes[16 + 14] = bytes[9];
            bytes[16 + 15] = bytes[8];

            // Upper 64 bits (bytes 0-7)
            long upper = BitConverter.ToInt64(bytes, 16 + 0);

            // Lower 64 bits (bytes 8-15)
            ulong lower = BitConverter.ToUInt64(bytes, 16 + 8);

            // Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
            upper ^= ((long)1 << 63);

            return new DuckDBHugeInt(lower, upper);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bytes);
        }
    }
}