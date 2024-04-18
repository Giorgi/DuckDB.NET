using System;
using System.Numerics;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal unsafe class DataChunkVectorWriter(IntPtr vector, void* vectorData)
{
    private unsafe ulong* validity;

    public unsafe void AppendNull(ulong rowIndex)
    {
        if (validity == default)
        {
            NativeMethods.Vectors.DuckDBVectorEnsureValidityWritable(vector);
            validity = NativeMethods.Vectors.DuckDBVectorGetValidity(vector);
        }

        NativeMethods.ValidityMask.DuckDBValiditySetRowValidity(validity, rowIndex, false);
    }

    public unsafe void AppendValue<T>(T value, ulong rowIndex) where T : unmanaged
    {
        ((T*)vectorData)[rowIndex] = value;
    }

    public void AppendString(string value, ulong rowIndex)
    {
        using var unmanagedString = value.ToUnmanagedString();
        NativeMethods.Vectors.DuckDBVectorAssignStringElement(vector, rowIndex, unmanagedString);
    }

    public void AppendBlob(byte* value, int length, ulong rowIndex)
    {
        NativeMethods.Vectors.DuckDBVectorAssignStringElementLength(vector, rowIndex, value, length);
    }
}

class DataChunkDecimalVectorWriter : DataChunkVectorWriter
{
    private readonly byte scale;
    private readonly DuckDBType decimalType;

    public unsafe DataChunkDecimalVectorWriter(IntPtr vector, void* vectorData, DuckDBLogicalType logicalType) : base(vector, vectorData)
    {
        scale = NativeMethods.LogicalType.DuckDBDecimalScale(logicalType);
        decimalType = NativeMethods.LogicalType.DuckDBDecimalInternalType(logicalType);
    }

    public void AppendDecimal(decimal value, ulong rowIndex)
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
                var bigInteger = BigInteger.Multiply(new BigInteger(value), new BigInteger(power));
                AppendValue(new DuckDBHugeInt(bigInteger), rowIndex);
                break;
        }
    }
}