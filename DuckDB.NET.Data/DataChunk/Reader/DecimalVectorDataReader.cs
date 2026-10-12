namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class DecimalVectorDataReader : VectorDataReaderBase
{
    private readonly Int128 remainderShift;
    private readonly DuckDBType decimalType;

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
            remainderShift = DecimalExtensions.Int128PowersOfTen[Scale - DecimalExtensions.MaxDecimalScale];
        }
    }

    internal byte Scale { get; }

    internal byte Precision { get; }

    internal override decimal ReadDecimal(ulong offset) => GetValidValue<decimal>(offset);

    protected override T GetValidValue<T>(ulong offset)
    {
        if (DuckDBType != DuckDBType.Decimal)
        {
            return base.GetValidValue<T>(offset);
        }

        if (typeof(T) == typeof(BigInteger))
        {
            return (T)(object)GetBigInteger(offset);
        }

        if (typeof(T) == typeof(DuckDBDecimal))
        {
            return (T)(object)GetDuckDBDecimal(offset);
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

        if (targetType == typeof(BigInteger))
        {
            return GetBigInteger(offset);
        }

        if (targetType == typeof(DuckDBDecimal))
        {
            return GetDuckDBDecimal(offset);
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
                return GetWideDecimal(offset);
            default: throw InvalidStorageType();
        }
    }

    // A DECIMAL of more than 18 digits, which DuckDB stores as a HUGEINT. It is a method of its own because the
    // division and the try block need a large stack frame, and inside GetDecimal that frame is set up and
    // cleared on every call, also for the three storage sizes that do not need it.
    private decimal GetWideDecimal(ulong offset)
    {
        // A HUGEINT is an Int128. Dividing it as one allocates nothing; a BigInteger allocates for
        // the value, the quotient and the remainder.
        var raw = GetFieldData<DuckDBHugeInt>(offset);

        var (quotient, remainder) = Int128.DivRem(new Int128((ulong)raw.Upper, raw.Lower), DecimalExtensions.Int128PowersOfTen[Scale]);

        try
        {
            var result = (decimal)quotient;

            if (Scale <= DecimalExtensions.MaxDecimalScale)
            {
                result += decimal.Divide((decimal)remainder, DecimalExtensions.PowersOfTen[Scale]);
            }
            else
            {
                // Scale > 28: remainder can exceed decimal range.
                // Shift it down to fit, losing digits beyond decimal's 28-29 digit precision.
                var shiftedRemainder = remainder / remainderShift;
                result += (decimal)shiftedRemainder / DecimalExtensions.PowersOfTen[DecimalExtensions.MaxDecimalScale];
            }

            return result;
        }
        catch (OverflowException exception)
        {
            throw new OverflowException($"Value in column {ColumnName} exceeds the range of System.Decimal. Use GetFieldValue<BigInteger>() or GetFieldValue<DuckDBDecimal>() to read it.", exception);
        }
    }

    private BigInteger GetBigInteger(ulong offset)
    {
        var unscaledValue = GetUnscaledValue(offset);

        if (Scale == 0)
        {
            return unscaledValue;
        }

        var quotient = BigInteger.DivRem(unscaledValue, DecimalExtensions.BigIntPowersOfTen[Scale], out var remainder);

        if (!remainder.IsZero)
        {
            throw new InvalidCastException($"Cannot cast a value with a non-zero fractional part to {nameof(BigInteger)} in column {ColumnName}");
        }

        return quotient;
    }

    private DuckDBDecimal GetDuckDBDecimal(ulong offset)
    {
        var value = decimalType switch
        {
            DuckDBType.SmallInt => ToHugeInt(GetFieldData<short>(offset)),
            DuckDBType.Integer => ToHugeInt(GetFieldData<int>(offset)),
            DuckDBType.BigInt => ToHugeInt(GetFieldData<long>(offset)),
            DuckDBType.HugeInt => GetFieldData<DuckDBHugeInt>(offset),
            _ => throw InvalidStorageType()
        };

        return new DuckDBDecimal(Precision, Scale, value);
    }

    private BigInteger GetUnscaledValue(ulong offset) => decimalType switch
    {
        DuckDBType.SmallInt => GetFieldData<short>(offset),
        DuckDBType.Integer => GetFieldData<int>(offset),
        DuckDBType.BigInt => GetFieldData<long>(offset),
        DuckDBType.HugeInt => GetFieldData<DuckDBHugeInt>(offset).ToBigInteger(),
        _ => throw InvalidStorageType()
    };

    // A method, so that the message is not built inside GetDecimal: the buffer for it would be part of that
    // method's stack frame and be cleared on every call.
    private DuckDBException InvalidStorageType() => new DuckDBException($"Invalid storage type {decimalType} ({(int)decimalType}) for decimal column {ColumnName}");

    private static DuckDBHugeInt ToHugeInt(long value) => new(unchecked((ulong)value), value < 0 ? -1 : 0);
}
