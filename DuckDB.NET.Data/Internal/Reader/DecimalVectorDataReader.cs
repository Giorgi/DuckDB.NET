using System;
using System.Numerics;

namespace DuckDB.NET.Data.Internal.Reader;

internal class DecimalVectorDataReader : VectorDataReader
{
    private readonly byte scale;
    private readonly DuckDBType decimalType;

    internal unsafe DecimalVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType) : base(vector, dataPointer, validityMaskPointer, columnType)
    {
        using var logicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(vector);
        scale = NativeMethods.LogicalType.DuckDBDecimalScale(logicalType);
        decimalType = NativeMethods.LogicalType.DuckDBDecimalInternalType(logicalType);
    }

    internal override decimal GetDecimal(ulong offset)
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
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
}