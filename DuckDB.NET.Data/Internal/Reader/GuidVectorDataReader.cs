using System;
using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Data.Internal.Reader;

internal class GuidVectorDataReader : VectorDataReaderBase
{
    private static readonly char[] HexDigits = "0123456789abcdef".ToCharArray();

    internal unsafe GuidVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

    internal override unsafe T GetValue<T>(ulong offset)
    {
        if (DuckDBType != DuckDBType.Uuid)
        {
            return base.GetValue<T>(offset);
        }

        if (IsValid(offset))
        {
            var input = GetFieldData<DuckDBHugeInt>(offset);

            var guid = ConvertToGuid(input);
            return (T)(object)guid;
        }

        var (isNullable, _) = TypeExtensions.IsNullableValueType<T>();
        if (isNullable)
        {
            return default!;
        }

        throw new InvalidCastException($"Column '{ColumnName}' value is null");
    }

    internal override unsafe object GetValue(ulong offset, Type targetType)
    {
        if (DuckDBType != DuckDBType.Uuid)
        {
            return base.GetValue(offset, targetType);
        }

        var input = GetFieldData<DuckDBHugeInt>(offset);

        var guid = ConvertToGuid(input);
        return guid;
    }

    //Ported from duckdb source code UUID::ToString
    //https://github.com/duckdb/duckdb/blob/9c91b3a329073ea1767b0aaff94b51da98dd03e2/src/common/types/uuid.cpp#L56
    private static Guid ConvertToGuid(DuckDBHugeInt input)
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
}