using System.Buffers.Binary;

namespace DuckDB.NET.Data.Extensions;

internal static class GuidConverter
{
    // DuckDB stores a UUID as the 128-bit number its text form spells, with the top bit flipped. So the upper half
    // holds the first three groups of the text (8, 4 and 4 hex digits) and the lower half the last two, both with
    // the first digit in the highest bits. Those groups are the parts a Guid is constructed from.
    public static Guid ConvertToGuid(this DuckDBHugeInt input)
    {
        // Reverse the bit flip on the upper 64 bits
        var upper = input.Upper ^ ((long)1 << 63);
        var lower = input.Lower;

        return new Guid((int)(upper >> 32), (short)(upper >> 16), (short)upper,
            (byte)(lower >> 56), (byte)(lower >> 48), (byte)(lower >> 40), (byte)(lower >> 32),
            (byte)(lower >> 24), (byte)(lower >> 16), (byte)(lower >> 8), (byte)lower);
    }


    //https://github.com/duckdb/duckdb/blob/9c91b3a329073ea1767b0aaff94b51da98dd03e2/src/common/types/uuid.cpp#L6
    public static DuckDBHugeInt ToHugeInt(this Guid guid, bool flip = true)
    {
        // The bytes of the Guid in the order of its text form, which is the number DuckDB stores
        Span<byte> bytes = stackalloc byte[16];

        guid.TryWriteBytes(bytes, bigEndian: true, out _);

        var upper = BinaryPrimitives.ReadInt64BigEndian(bytes);
        var lower = BinaryPrimitives.ReadUInt64BigEndian(bytes[8..]);

        //Do not flip if we are passing it to duckdb_create_uuid. That function will flip it for us.
        if (flip)
        {
            // Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
            upper ^= (long)1 << 63; 
        }

        return new DuckDBHugeInt(lower, upper);
    }
}