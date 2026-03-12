namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class DecimalVectorDataReader : VectorDataReaderBase
{
    private readonly BigInteger bigIntRemainderShift;
    private readonly DuckDBType decimalType;
    private readonly NumericVectorDataReader numericVectorDataReader;

    internal unsafe DecimalVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        using var logicalType = NativeMethods.Vectors.DuckDBVectorGetColumnType(vector);
        Scale = NativeMethods.LogicalType.DuckDBDecimalScale(logicalType);
        Precision = NativeMethods.LogicalType.DuckDBDecimalWidth(logicalType);
        decimalType = NativeMethods.LogicalType.DuckDBDecimalInternalType(logicalType);

        // SmallInt/Integer/BigInt paths have scale ≤ 18, always within range.
        if (Scale > DecimalExtensions.MaxDecimalScale)
        {
            // Scale > 28: precompute the shift divisor for truncating the remainder to decimal range.
            bigIntRemainderShift = DecimalExtensions.BigIntPowersOfTen[Scale - DecimalExtensions.MaxDecimalScale];
        }

        numericVectorDataReader = new NumericVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName);
    }

    internal byte Scale { get; }

    internal byte Precision { get; }

    protected override T GetValidValue<T>(ulong offset)
    {
        if (DuckDBType != DuckDBType.Decimal)
        {
            return base.GetValidValue<T>(offset);
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
        switch (decimalType)
        {
            case DuckDBType.SmallInt:
                {
                    var raw = GetFieldData<short>(offset);
                    return new decimal(Math.Abs(raw), 0, 0, raw < 0, Scale);
                }
            case DuckDBType.Integer:
                {
                    var raw = GetFieldData<int>(offset);
                    return new decimal(Math.Abs(raw), 0, 0, raw < 0, Scale);
                }
            case DuckDBType.BigInt:
                {
                    var raw = GetFieldData<long>(offset);
                    var abs = (ulong)Math.Abs(raw);
                    return new decimal((int)abs, (int)(abs >> 32), 0, raw < 0, Scale);
                }
            case DuckDBType.HugeInt:
                {
                    var hugeInt = numericVectorDataReader.GetBigInteger(offset, false);

                    var result = (decimal)BigInteger.DivRem(hugeInt, DecimalExtensions.BigIntPowersOfTen[Scale], out var remainder);

                    if (Scale <= DecimalExtensions.MaxDecimalScale)
                    {
                        result += decimal.Divide((decimal)remainder, DecimalExtensions.PowersOfTen[Scale]);
                    }
                    else
                    {
                        // Scale > 28: remainder can exceed decimal range.
                        // Shift it down to fit, losing digits beyond decimal's 28-29 digit precision.
                        var shiftedRemainder = remainder / bigIntRemainderShift;
                        result += (decimal)shiftedRemainder / DecimalExtensions.PowersOfTen[DecimalExtensions.MaxDecimalScale];
                    }

                    return result;
                }
            default: throw new DuckDBException($"Invalid type {DuckDBType} ({(int)DuckDBType}) for column {ColumnName}");
        }
    }

    internal override void Reset(IntPtr vector)
    {
        base.Reset(vector);
        numericVectorDataReader.Reset(vector);
    }

    public override void Dispose()
    {
        numericVectorDataReader.Dispose();
        base.Dispose();
    }
}
