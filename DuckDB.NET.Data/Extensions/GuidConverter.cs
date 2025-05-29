using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Extensions;

internal static class GuidConverter
{
    private const int GuidSize = 16;

    // First 4 bytes (little-endian), Next 4 bytes (little-endian), Last 8 bytes (big-endian)
    private static readonly int[] GuidByteOrder = [6, 7, 4, 5, 0, 1, 2, 3, 15, 14, 13, 12, 11, 10, 9, 8];

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
        for (int i = 0; i < GuidSize; i++)
        {
            bytes[GuidByteOrder[i]] = bytes[i + GuidSize];
        }

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

        // Reconstruct the Guid bytes (reverse the original byte reordering)
        for (int i = 0; i < GuidSize; i++)
        {
            bytes[i + GuidSize] = bytes[GuidByteOrder[i]];
        }

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