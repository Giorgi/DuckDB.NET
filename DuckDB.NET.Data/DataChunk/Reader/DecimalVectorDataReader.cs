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

    internal override byte ReadByte(ulong offset) => GetValidValue<byte>(offset);
    internal override short ReadInt16(ulong offset) => GetValidValue<short>(offset);
    internal override int ReadInt32(ulong offset) => GetValidValue<int>(offset);
    internal override long ReadInt64(ulong offset) => GetValidValue<long>(offset);
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

        if (TypeExtensions.IsIntegralNumericType<T>())
        {
            return GetInteger<T>(offset);
        }

        // A decimal goes to every type that can hold one (decimal itself and object), as GetValue(offset, Type) does,
        // and the others are refused with the same message. For decimal the test is a constant for the JIT.
        if (typeof(T) != typeof(decimal) && !typeof(T).IsAssignableFrom(typeof(decimal)))
        {
            throw CannotCast(DuckDBType.ToString(), typeof(T));
        }

        return (T)(object)GetDecimal(offset);
    }

    internal override object GetValueCore(ulong offset, Type targetType)
    {
        if (DuckDBType != DuckDBType.Decimal)
        {
            return base.GetValueCore(offset, targetType);
        }

        // The column's own type first: GetValue(i), the indexer and every read as object ask for it.
        if (targetType == typeof(decimal))
        {
            return GetDecimal(offset);
        }

        if (targetType == typeof(BigInteger))
        {
            return GetBigInteger(offset);
        }

        if (targetType == typeof(DuckDBDecimal))
        {
            return GetDuckDBDecimal(offset);
        }

        // The integer types as on the typed path. The class is sealed, so these calls are bound directly.
        if (targetType == typeof(sbyte))  return GetValidValue<sbyte>(offset);
        if (targetType == typeof(byte))   return GetValidValue<byte>(offset);
        if (targetType == typeof(short))  return GetValidValue<short>(offset);
        if (targetType == typeof(ushort)) return GetValidValue<ushort>(offset);
        if (targetType == typeof(int))    return GetValidValue<int>(offset);
        if (targetType == typeof(uint))   return GetValidValue<uint>(offset);
        if (targetType == typeof(long))   return GetValidValue<long>(offset);
        if (targetType == typeof(ulong))  return GetValidValue<ulong>(offset);

        // The typed path unboxes the decimal as the type it is asked for, so it gives a decimal to every type that
        // can hold one (decimal itself, above, decimal? and its interfaces) and refuses the others. This does the same.
        if (targetType.IsAssignableFrom(typeof(decimal)))
        {
            return GetDecimal(offset);
        }

        throw CannotCast(DuckDBType.ToString(), targetType);
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
        var raw = GetFieldData<DuckDBHugeInt>(offset).ToInt128();

        var (quotient, remainder) = Int128.DivRem(raw, DecimalExtensions.Int128PowersOfTen[Scale]);

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

    private BigInteger GetBigInteger(ulong offset) => GetWhole(offset, typeof(BigInteger));

    // A DECIMAL read as an integer type: a whole value that fits reads, a value with a fraction is refused, and so is a
    // value out of range (2.0 item 5; before 2.0 every read as sbyte to ulong was refused). The same rule as FLOAT and
    // DOUBLE. The value is never made a decimal, so a DECIMAL(38) beyond the range of decimal reads if T holds it.
    private T GetInteger<T>(ulong offset)
    {
        var whole = GetWhole(offset, typeof(T));

        try
        {
            return NumericConverter.Convert<Int128, T>(whole);
        }
        catch (OverflowException)
        {
            throw CannotCast(DuckDBType.ToString(), typeof(T));
        }
    }

    // The value as a whole number; one with a fraction is refused for the type it was read as.
    private Int128 GetWhole(ulong offset, Type target)
    {
        var unscaledValue = GetUnscaledValue(offset);

        if (Scale == 0)
        {
            return unscaledValue;
        }

        // Divided as an Int128, as in GetWideDecimal: every DECIMAL fits one, and so does every power of ten.
        var (quotient, remainder) = Int128.DivRem(unscaledValue, DecimalExtensions.Int128PowersOfTen[Scale]);

        if (remainder != 0)
        {
            throw CannotCastFraction(target);
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

    private Int128 GetUnscaledValue(ulong offset)
    {
        return decimalType switch
        {
            DuckDBType.SmallInt => GetFieldData<short>(offset),
            DuckDBType.Integer => GetFieldData<int>(offset),
            DuckDBType.BigInt => GetFieldData<long>(offset),
            DuckDBType.HugeInt => GetFieldData<DuckDBHugeInt>(offset).ToInt128(),
            _ => throw InvalidStorageType()
        };
    }

    // A method, so that the message is not built inside GetDecimal: the buffer for it would be part of that
    // method's stack frame and be cleared on every call.
    private DuckDBException InvalidStorageType() => new($"Invalid storage type {decimalType} ({(int)decimalType}) for decimal column {ColumnName}");

    private static DuckDBHugeInt ToHugeInt(long value) => new(unchecked((ulong)value), value < 0 ? -1 : 0);
}
