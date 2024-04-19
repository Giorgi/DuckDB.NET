using System;
using DuckDB.NET.Data.Internal.Reader;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Extensions;

internal static class GuidConverter
{
    private static readonly char[] HexDigits = "0123456789abcdef".ToCharArray();

    //Ported from duckdb source code UUID::ToString
    //https://github.com/duckdb/duckdb/blob/9c91b3a329073ea1767b0aaff94b51da98dd03e2/src/common/types/uuid.cpp#L56
    public static Guid ConvertToGuid(this DuckDBHugeInt input)
    {
        var buffer = new char[36];

        var upper = input.Upper ^ (((long)1) << 63);
        var position = 0;

        ByteToHex((ulong)(upper >> 56 & 0xFF));
        ByteToHex((ulong)(upper >> 48 & 0xFF));
        ByteToHex((ulong)(upper >> 40 & 0xFF));
        ByteToHex((ulong)(upper >> 32 & 0xFF));

        buffer[position++] = '-';

        ByteToHex((ulong)(upper >> 24 & 0xFF));
        ByteToHex((ulong)(upper >> 16 & 0xFF));

        buffer[position++] = '-';

        ByteToHex((ulong)(upper >> 8 & 0xFF));
        ByteToHex((ulong)(upper & 0xFF));

        buffer[position++] = '-';

        ByteToHex(input.Lower >> 56 & 0xFF);
        ByteToHex(input.Lower >> 48 & 0xFF);

        buffer[position++] = '-';

        ByteToHex(input.Lower >> 40 & 0xFF);
        ByteToHex(input.Lower >> 32 & 0xFF);
        ByteToHex(input.Lower >> 24 & 0xFF);
        ByteToHex(input.Lower >> 16 & 0xFF);
        ByteToHex(input.Lower >> 8 & 0xFF);
        ByteToHex(input.Lower & 0xFF);

        return new Guid(new string(buffer));

        void ByteToHex(ulong value)
        {
            buffer[position++] = HexDigits[(value >> 4) & 0xf];
            buffer[position++] = HexDigits[value & 0xf];
        }
    }


    //https://github.com/duckdb/duckdb/blob/9c91b3a329073ea1767b0aaff94b51da98dd03e2/src/common/types/uuid.cpp#L6
    public static DuckDBHugeInt ToHugeInt(this Guid guid)
    {
        char HexToChar(char ch)
        {
            return ch switch
            {
                >= '0' and <= '9' => (char)(ch - '0'),
                >= 'a' and <= 'f' => (char)(10 + ch - 'a'),
                >= 'A' and <= 'F' => (char)(10 + ch - 'A'),
                _ => (char)0
            };
        }

        ulong lower = 0;
        long upper = 0;

        var str = guid.ToString("N");

        for (var index = 0; index < str.Length; index++)
        {
            if (index >= 16)
            {
                lower = (lower << 4) | HexToChar(str[index]);
            }
            else
            {
                upper = (upper << 4) | HexToChar(str[index]);
            }
        }

        // Flip the first bit to make `order by uuid` same as `order by uuid::varchar`
        upper ^= ((long)1 << 63);
        return new DuckDBHugeInt(lower, upper);
    }
}