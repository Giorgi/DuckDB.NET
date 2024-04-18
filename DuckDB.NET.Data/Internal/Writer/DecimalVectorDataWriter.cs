using System;
using System.Numerics;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal unsafe class DecimalVectorDataWriter(IntPtr vector, void* vectorData, DuckDBLogicalType logicalType) : VectorDataWriterBase(vector, vectorData)
{
    private readonly byte scale = NativeMethods.LogicalType.DuckDBDecimalScale(logicalType);
    private readonly DuckDBType decimalType = NativeMethods.LogicalType.DuckDBDecimalInternalType(logicalType);

    public void Append(decimal value, ulong rowIndex)
    {
        var power = Math.Pow(10, scale);

        switch (decimalType)
        {
            case DuckDBType.SmallInt:
                AppendValue<short>((short)decimal.Multiply(value, new decimal(power)), rowIndex);
                break;
            case DuckDBType.Integer:
                AppendValue<int>((int)decimal.Multiply(value, new decimal(power)), rowIndex);
                break;
            case DuckDBType.BigInt:
                AppendValue<long>((long)decimal.Multiply(value, new decimal(power)), rowIndex);
                break;
            case DuckDBType.HugeInt:
                var integralPart = decimal.Truncate(value);
                var fractionalPart = value - integralPart;

                var result = BigInteger.Multiply(new BigInteger(integralPart), new BigInteger(power));

                result  += new BigInteger(decimal.Multiply(fractionalPart, (decimal)power));
                
                AppendValue(new DuckDBHugeInt(result), rowIndex);
                break;
        }
    }
}