using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal unsafe class GuidVectorDataWriter(IntPtr vector, void* vectorData) : VectorDataWriterBase(vector, vectorData)
{
    public void AppendValue(Guid value, ulong rowIndex)
    {
        AppendValue(ToHugeInt(value), rowIndex);
    }

    //https://github.com/duckdb/duckdb/blob/9c91b3a329073ea1767b0aaff94b51da98dd03e2/src/common/types/uuid.cpp#L6
    DuckDBHugeInt ToHugeInt(Guid guid)
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