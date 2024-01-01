using System;
using System.Numerics;
using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Data.Internal.Reader;

internal class DecimalVectorDataReader : NumericVectorDataReader
{
    private readonly byte scale;
    private readonly DuckDBType decimalType;

    internal unsafe DecimalVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        using var logicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(vector);
        scale = NativeMethods.LogicalType.DuckDBDecimalScale(logicalType);
        decimalType = NativeMethods.LogicalType.DuckDBDecimalInternalType(logicalType);
    }

    internal override T GetValue<T>(ulong offset)
    {
        if (DuckDBType != DuckDBType.Decimal)
        {
            return base.GetValue<T>(offset);
        }

        if (IsValid(offset))
        {
            var value = GetDecimal(offset);
            return (T)(object)value; //JIT will optimize the casts at least for not nullable T
        }

        var (isNullable, _) = TypeExtensions.IsNullable<T>();
        if (isNullable)
        {
            return default!;
        }

        throw new InvalidCastException($"Column '{ColumnName}' value is null");
    }

    internal override object GetValue(ulong offset, Type targetType)
    {
        if (DuckDBType != DuckDBType.Decimal)
        {
            return base.GetValue(offset, targetType);
        }

        return GetDecimal(offset);
    }

    private decimal GetDecimal(ulong offset)
    {
        var pow = (decimal)Math.Pow(10, scale);
        switch (decimalType)
        {
            case DuckDBType.SmallInt:
                return decimal.Divide(GetFieldData<short>(offset), pow);
            case DuckDBType.Integer:
                return decimal.Divide(GetFieldData<int>(offset), pow);
            case DuckDBType.BigInt:
                return decimal.Divide(GetFieldData<long>(offset), pow);
            case DuckDBType.HugeInt:
                {
                    var hugeInt = GetBigInteger(offset);

                    var result = (decimal)BigInteger.DivRem(hugeInt, (BigInteger)pow, out var remainder);

                    result += decimal.Divide((decimal)remainder, pow);
                    return result;
                }
            default: throw new DuckDBException($"Invalid type {DuckDBType} ({(int)DuckDBType}) for column {ColumnName}");
        }
    }
}