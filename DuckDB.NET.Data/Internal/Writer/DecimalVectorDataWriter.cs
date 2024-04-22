using System;
using System.Numerics;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal sealed unsafe class DecimalVectorDataWriter(IntPtr vector, void* vectorData, DuckDBLogicalType logicalType, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    private readonly byte scale = NativeMethods.LogicalType.DuckDBDecimalScale(logicalType);
    private readonly DuckDBType decimalType = NativeMethods.LogicalType.DuckDBDecimalInternalType(logicalType);

    public void AppendValue(decimal value, ulong rowIndex)
    {
        var power = Math.Pow(10, scale);

        switch (decimalType)
        {
            case DuckDBType.SmallInt:
                AppendValueInternal<short>((short)decimal.Multiply(value, new decimal(power)), rowIndex);
                break;
            case DuckDBType.Integer:
                AppendValueInternal<int>((int)decimal.Multiply(value, new decimal(power)), rowIndex);
                break;
            case DuckDBType.BigInt:
                AppendValueInternal<long>((long)decimal.Multiply(value, new decimal(power)), rowIndex);
                break;
            case DuckDBType.HugeInt:
                var integralPart = decimal.Truncate(value);
                var fractionalPart = value - integralPart;

                var result = BigInteger.Multiply(new BigInteger(integralPart), new BigInteger(power));

                result  += new BigInteger(decimal.Multiply(fractionalPart, (decimal)power));
                
                AppendValueInternal(new DuckDBHugeInt(result), rowIndex);
                break;
        }
    }
}