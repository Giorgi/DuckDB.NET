namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class DecimalVectorDataReader : VectorDataReaderBase
{
    private readonly DuckDBType decimalType;
    private readonly NumericVectorDataReader numericVectorDataReader;

    internal unsafe DecimalVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        using var logicalType = NativeMethods.Vectors.DuckDBVectorGetColumnType(vector);
        Scale = NativeMethods.LogicalType.DuckDBDecimalScale(logicalType);
        Precision = NativeMethods.LogicalType.DuckDBDecimalWidth(logicalType);
        decimalType = NativeMethods.LogicalType.DuckDBDecimalInternalType(logicalType);

        numericVectorDataReader = new NumericVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName);
    }

    internal byte Scale { get; }

    internal byte Precision { get; }

    protected override T GetValidValue<T>(ulong offset, Type targetType)
    {
        if (DuckDBType != DuckDBType.Decimal)
        {
            return base.GetValidValue<T>(offset, targetType);
        }

        var value = GetDecimal(offset);
        return (T)(object)value; //JIT will optimize the casts at least for not nullable T
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
        var pow = (decimal)Math.Pow(10, Scale);
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
                    var hugeInt = numericVectorDataReader.GetBigInteger(offset, false);

                    var result = (decimal)BigInteger.DivRem(hugeInt, (BigInteger)pow, out var remainder);

                    result += decimal.Divide((decimal)remainder, pow);
                    return result;
                }
            default: throw new DuckDBException($"Invalid type {DuckDBType} ({(int)DuckDBType}) for column {ColumnName}");
        }
    }

    public override void Dispose()
    {
        numericVectorDataReader.Dispose();
        base.Dispose();
    }
}