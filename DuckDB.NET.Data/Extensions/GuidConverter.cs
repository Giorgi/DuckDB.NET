using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Extensions;

internal static class GuidConverter
{
    private const int GuidSize = 16;

    public static unsafe Guid ConvertToGuid(this DuckDBHugeInt input)
    {
        Span<byte> bytes = stackalloc byte[32];

        // Reverse the bit flip on the upper 64 bits
        var upper = input.Upper ^ ((long)1 << 63);

#if NET6_0_OR_GREATER
        // Write upper 64 bits (bytes 0-7)
        BitConverter.TryWriteBytes(bytes[GuidSize..], upper);

        // Write lower 64 bits (bytes 8-15)
        BitConverter.TryWriteBytes(bytes[(GuidSize + 8)..], input.Lower);
#else
        var data = BitConverter.GetBytes(upper);
        data.CopyTo(bytes);

        data = BitConverter.GetBytes(input.Lower);
        data.CopyTo(bytes.Slice(GuidSize + 8));
#endif

        // Reconstruct the Guid bytes (reverse the original byte reordering)

        // First 4 bytes (little-endian)
        bytes[6] = bytes[GuidSize + 0];
        bytes[7] = bytes[GuidSize + 1];
        bytes[4] = bytes[GuidSize + 2];
        bytes[5] = bytes[GuidSize + 3];

        // Next 4 bytes (little-endian)
        bytes[0] = bytes[GuidSize + 4];
        bytes[1] = bytes[GuidSize + 5];
        bytes[2] = bytes[GuidSize + 6];
        bytes[3] = bytes[GuidSize + 7];

        // Last 8 bytes (big-endian)
        bytes[15] = bytes[GuidSize + 8];
        bytes[14] = bytes[GuidSize + 9];
        bytes[13] = bytes[GuidSize + 10];
        bytes[12] = bytes[GuidSize + 11];
        bytes[11] = bytes[GuidSize + 12];
        bytes[10] = bytes[GuidSize + 13];
        bytes[9] = bytes[GuidSize + 14];
        bytes[8] = bytes[GuidSize + 15];

        // Create Guid from the first 16 bytes
#if NET6_0_OR_GREATER
        return new Guid(bytes[..GuidSize]);
#else
        return new Guid(bytes.Slice(0, GuidSize).ToArray());
#endif
    }


    //https://github.com/duckdb/duckdb/blob/9c91b3a329073ea1767b0aaff94b51da98dd03e2/src/common/types/uuid.cpp#L6
    public static DuckDBHugeInt ToHugeInt(this Guid guid)
    {
        Span<byte> bytes = stackalloc byte[32];

#if NET6_0_OR_GREATER
        guid.TryWriteBytes(bytes);
#else
        var byteArray = guid.ToByteArray();
        byteArray.AsSpan().CopyTo(bytes);
#endif
        bytes[GuidSize + 0] = bytes[6]; // First 4 bytes (little-endian)
        bytes[GuidSize + 1] = bytes[7];
        bytes[GuidSize + 2] = bytes[4];
        bytes[GuidSize + 3] = bytes[5];

        bytes[GuidSize + 4] = bytes[0]; // Next 4 bytes (little-endian)
        bytes[GuidSize + 5] = bytes[1];
        bytes[GuidSize + 6] = bytes[2];
        bytes[GuidSize + 7] = bytes[3];

        bytes[GuidSize + 8] = bytes[15]; // Big endian 
        bytes[GuidSize + 9] = bytes[14];
        bytes[GuidSize + 10] = bytes[13];
        bytes[GuidSize + 11] = bytes[12];
        bytes[GuidSize + 12] = bytes[11];
        bytes[GuidSize + 13] = bytes[10];
        bytes[GuidSize + 14] = bytes[9];
        bytes[GuidSize + 15] = bytes[8];

#if NET6_0_OR_GREATER
        // Upper 64 bits (bytes 0-7)
        long upper = BitConverter.ToInt64(bytes[GuidSize..]);

        // Lower 64 bits (bytes 8-15)
        ulong lower = BitConverter.ToUInt64(bytes[(GuidSize + 8)..]);
#else
        var array = bytes.ToArray();

        long upper = BitConverter.ToInt64(array, GuidSize);
        ulong lower = BitConverter.ToUInt64(array, GuidSize + 8);
#endif

        // Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
        upper ^= (long)1 << 63;

        return new DuckDBHugeInt(lower, upper);
    }
}