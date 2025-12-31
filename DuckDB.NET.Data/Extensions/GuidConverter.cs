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

        // Write upper 64 bits (bytes 0-7)
        BitConverter.TryWriteBytes(bytes[GuidSize..], upper);

        // Write lower 64 bits (bytes 8-15)
        BitConverter.TryWriteBytes(bytes[(GuidSize + 8)..], input.Lower);

        // Reconstruct the Guid bytes (reverse the original byte reordering)
        for (var i = 0; i < GuidSize; i++)
        {
            bytes[GuidByteOrder[i]] = bytes[i + GuidSize];
        }

        // Create Guid from the first 16 bytes
        return new Guid(bytes[..GuidSize]);
    }


    //https://github.com/duckdb/duckdb/blob/9c91b3a329073ea1767b0aaff94b51da98dd03e2/src/common/types/uuid.cpp#L6
    public static DuckDBHugeInt ToHugeInt(this Guid guid, bool flip = true)
    {
        Span<byte> bytes = stackalloc byte[32];

        guid.TryWriteBytes(bytes);

        // Reconstruct the Guid bytes (reverse the original byte reordering)
        for (var i = 0; i < GuidSize; i++)
        {
            bytes[i + GuidSize] = bytes[GuidByteOrder[i]];
        }

        // Upper 64 bits (bytes 0-7)
        var upper = BitConverter.ToInt64(bytes[GuidSize..]);

        // Lower 64 bits (bytes 8-15)
        var lower = BitConverter.ToUInt64(bytes[(GuidSize + 8)..]);
        //Do not flip if we are passing it to duckdb_create_uuid. That function will flip it for us.
        if (flip)
        {
            // Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
            upper ^= (long)1 << 63; 
        }

        return new DuckDBHugeInt(lower, upper);
    }
}