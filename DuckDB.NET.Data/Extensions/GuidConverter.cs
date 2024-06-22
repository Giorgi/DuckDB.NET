using System;
using DuckDB.NET.Data.Internal.Reader;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Extensions;

internal static class GuidConverter
{
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

#if NET6_0_OR_GREATER
        return Guid.ParseExact(buffer, GuidFormat);
#else
        return Guid.ParseExact(new string(buffer.ToArray()), GuidFormat);
#endif

        static void ByteToHex(Span<char> buffer, ref int position, ulong value)
        {
            buffer[position++] = HexDigits[(value >> 4) & 0xF];
            buffer[position++] = HexDigits[value & 0xF];
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