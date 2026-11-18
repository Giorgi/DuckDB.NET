using System.Globalization;
using System.Text;

namespace DuckDB.NET.Data.PreparedStatement;

internal static class ClrToDuckDBConverter
{
    private static readonly Dictionary<DbType, Func<object, DuckDBValue>> ValueCreators = new()
    {
        { DbType.Guid, value => NativeMethods.Value.DuckDBCreateUuid(((Guid)value).ToHugeInt(false)) },
        { DbType.Currency, value => DecimalToDuckDBValue((decimal)value) },
        { DbType.Boolean, value => NativeMethods.Value.DuckDBCreateBool((bool)value) },
        { DbType.SByte, value => NativeMethods.Value.DuckDBCreateInt8((sbyte)value) },
        { DbType.Int16, value => NativeMethods.Value.DuckDBCreateInt16((short)value) },
        { DbType.Int32, value => NativeMethods.Value.DuckDBCreateInt32((int)value) },
        { DbType.Int64, value => NativeMethods.Value.DuckDBCreateInt64((long)value) },
        { DbType.Byte, value => NativeMethods.Value.DuckDBCreateUInt8((byte)value) },
        { DbType.UInt16, value => NativeMethods.Value.DuckDBCreateUInt16((ushort)value) },
        { DbType.UInt32, value => NativeMethods.Value.DuckDBCreateUInt32((uint)value) },
        { DbType.UInt64, value => NativeMethods.Value.DuckDBCreateUInt64((ulong)value) },
        { DbType.Single, value => NativeMethods.Value.DuckDBCreateFloat((float)value) },
        { DbType.Double, value => NativeMethods.Value.DuckDBCreateDouble((double)value) },
        { DbType.String, value => CreateVarchar((string?)value) },
        { DbType.VarNumeric, value => value switch
            {
                Int128 int128 => NativeMethods.Value.DuckDBCreateHugeInt(new DuckDBHugeInt(int128)),
                UInt128 uint128 => NativeMethods.Value.DuckDBCreateUHugeInt(new DuckDBUHugeInt(uint128)),
                _ => BigIntegerWithoutTargetType((BigInteger)value)
            }
        },
        { DbType.Binary, value =>
            {
                var bytes = (byte[])value;
                return NativeMethods.Value.DuckDBCreateBlob(bytes, bytes.Length);
            }
        },
        { DbType.Date, value =>
            {
                var date = value is DateOnly dateOnly ? dateOnly.ToDuckDBDate() : ((DuckDBDateOnly)value).ToDuckDBDate();
                return NativeMethods.Value.DuckDBCreateDate(date);
            }
        },
        { DbType.Time, value =>
            {
                // A TimeSpan is a time of day here: TimeOnly.FromTimeSpan refuses a negative one or one of a day or more.
                var time = value switch
                {
                    TimeOnly timeOnly => timeOnly.ToDuckDBTime(),
                    TimeSpan timeSpan => TimeOnly.FromTimeSpan(timeSpan).ToDuckDBTime(),
                    _ => ((DuckDBTimeOnly)value).ToDuckDBTime()
                };
                return NativeMethods.Value.DuckDBCreateTime(time);
            }
        },
        { DbType.DateTime, value =>
            {
                var dateTime = value is DateTime dt ? dt.ToTimestampStruct(DuckDBType.Timestamp) : ((DuckDBTimestamp)value).ToDuckDBTimestampStruct();
                return NativeMethods.Value.DuckDBCreateTimestamp(dateTime);
            }
        },
        { DbType.DateTimeOffset, value => NativeMethods.Value.DuckDBCreateTimestampTz(((DateTimeOffset)value).ToTimestampStruct()) },
    };

    // Creates the value bound to a parameter. duckDBType is the parameter's type as DuckDB reports it from the statement
    // (Invalid when it cannot tell, as in "SELECT $1"); dbType is the parameter's DbType. The first rule that applies:
    // 1. null or DBNull is a NULL.
    // 2. An integer parameter takes an integer or an enum that fits, converted to its type. Any other value, and one
    //    that does not fit, goes as its own type, or as text when it has none.
    // 3. Another parameter type takes a value of the matching .NET type as that type, and a TimeSpan with no reported
    //    type is an INTERVAL. A collection is built item by item, each item by these rules with the type of the items.
    // 4. Anything else goes through the creator of the DbType, or as text formatted without the current culture.
    // Whatever does not have the parameter's type is cast by DuckDB, as it would cast a SQL literal: it rounds a
    // fraction and parses a string. A value that does not fit is refused on INSERT, matches no row in a comparison,
    // and fails to bind as a function argument. An enum is its number only for an integer parameter; elsewhere it
    // goes as its name.
    public static DuckDBValue ToDuckDBValue(this object? item, DuckDBLogicalType logicalType, DuckDBType duckDBType, DbType dbType)
    {
        if (item.IsNull())
        {
            return NativeMethods.Value.DuckDBCreateNullValue();
        }

        return (duckDBType, item) switch
        {
            (DuckDBType.Boolean, bool value) => NativeMethods.Value.DuckDBCreateBool(value),

            // An integer that fits is converted here, for two reasons. DuckDB binds the statement again when the value's
            // type differs from the parameter's, which makes an INSERT slower. And some function arguments take only
            // the exact type: lpad('2', $1, '0') reports INTEGER and does not bind with a BIGINT value, so a long works
            // there only because it is narrowed. Anything else goes as its own type (or as text), and DuckDB casts it
            // like a SQL literal.
            (DuckDBType.TinyInt, _) => TryConvertTo<sbyte>(item, out var result) ? NativeMethods.Value.DuckDBCreateInt8(result) : ValueOfItsOwnType(item),
            (DuckDBType.SmallInt, _) => TryConvertTo<short>(item, out var result) ? NativeMethods.Value.DuckDBCreateInt16(result) : ValueOfItsOwnType(item),
            (DuckDBType.Integer, _) => TryConvertTo<int>(item, out var result) ? NativeMethods.Value.DuckDBCreateInt32(result) : ValueOfItsOwnType(item),
            (DuckDBType.BigInt, _) => TryConvertTo<long>(item, out var result) ? NativeMethods.Value.DuckDBCreateInt64(result) : ValueOfItsOwnType(item),
            (DuckDBType.HugeInt, _) => TryConvertTo<Int128>(item, out var result) ? NativeMethods.Value.DuckDBCreateHugeInt(new DuckDBHugeInt(result)) : ValueOfItsOwnType(item),

            (DuckDBType.UnsignedTinyInt, _) => TryConvertTo<byte>(item, out var result) ? NativeMethods.Value.DuckDBCreateUInt8(result) : ValueOfItsOwnType(item),
            (DuckDBType.UnsignedSmallInt, _) => TryConvertTo<ushort>(item, out var result) ? NativeMethods.Value.DuckDBCreateUInt16(result) : ValueOfItsOwnType(item),
            (DuckDBType.UnsignedInteger, _) => TryConvertTo<uint>(item, out var result) ? NativeMethods.Value.DuckDBCreateUInt32(result) : ValueOfItsOwnType(item),
            (DuckDBType.UnsignedBigInt, _) => TryConvertTo<ulong>(item, out var result) ? NativeMethods.Value.DuckDBCreateUInt64(result) : ValueOfItsOwnType(item),
            (DuckDBType.UnsignedHugeInt, _) => TryConvertTo<UInt128>(item, out var result) ? NativeMethods.Value.DuckDBCreateUHugeInt(new DuckDBUHugeInt(result)) : ValueOfItsOwnType(item),

            (DuckDBType.Float, float value) => NativeMethods.Value.DuckDBCreateFloat(value),
            (DuckDBType.Double, double value) => NativeMethods.Value.DuckDBCreateDouble(value),

            (DuckDBType.Decimal, decimal value) => DecimalToDuckDBValue(value, logicalType),
            (DuckDBType.VarInt, BigInteger value) => BigIntegerToDuckDBValue(value),

            (DuckDBType.Varchar, string value) => CreateVarchar(value),
            (DuckDBType.Uuid, Guid value) => NativeMethods.Value.DuckDBCreateUuid(value.ToHugeInt(false)),

            (DuckDBType.Timestamp, DateTime value) => NativeMethods.Value.DuckDBCreateTimestamp(value.ToTimestampStruct(duckDBType)),
            (DuckDBType.TimestampS, DateTime value) => NativeMethods.Value.DuckDBCreateTimestampS(value.ToTimestampStruct(duckDBType)),
            (DuckDBType.TimestampMs, DateTime value) => NativeMethods.Value.DuckDBCreateTimestampMs(value.ToTimestampStruct(duckDBType)),
            (DuckDBType.TimestampNs, DateTime value) => NativeMethods.Value.DuckDBCreateTimestampNs(value.ToTimestampStruct(duckDBType)),
            (DuckDBType.TimestampTz, DateTime value) => NativeMethods.Value.DuckDBCreateTimestampTz(value.ToTimestampStruct(duckDBType)),
            (DuckDBType.TimestampTz, DateTimeOffset value) => NativeMethods.Value.DuckDBCreateTimestampTz(value.ToTimestampStruct()),
            (DuckDBType.Interval, TimeSpan value) => NativeMethods.Value.DuckDBCreateInterval(value),
            // No inferred type (a bare "?"): TimeSpan has no DbType, and its ToString() isn't interval text
            // DuckDB can parse once there are days ("1.01:01:01"). DbType.Time sends it as a time instead.
            (DuckDBType.Invalid, TimeSpan value) when dbType != DbType.Time => NativeMethods.Value.DuckDBCreateInterval(value),
            (DuckDBType.Date, DateTime value) => NativeMethods.Value.DuckDBCreateDate(value.ToDuckDBDate()),
            (DuckDBType.Date, DuckDBDateOnly value) => NativeMethods.Value.DuckDBCreateDate(value.ToDuckDBDate()),
            (DuckDBType.Time, DateTime value) => NativeMethods.Value.DuckDBCreateTime(value.ToDuckDBTime()),
            (DuckDBType.Time, DuckDBTimeOnly value) => NativeMethods.Value.DuckDBCreateTime(value.ToDuckDBTime()),
            (DuckDBType.Date, DateOnly value) => NativeMethods.Value.DuckDBCreateDate(value.ToDuckDBDate()),
            (DuckDBType.Time, TimeOnly value) => NativeMethods.Value.DuckDBCreateTime(value.ToDuckDBTime()),
            (DuckDBType.TimeNs, DateTime value) => NativeMethods.Value.DuckDBCreateTimeNs(DuckDBTimeNs.FromTicks(value.Ticks % TimeSpan.TicksPerDay)),
            (DuckDBType.TimeNs, DuckDBTimeOnly value) => NativeMethods.Value.DuckDBCreateTimeNs(DuckDBTimeNs.FromTicks(value.Ticks)),
            (DuckDBType.TimeNs, TimeOnly value) => NativeMethods.Value.DuckDBCreateTimeNs(DuckDBTimeNs.FromTicks(value.Ticks)),
            (DuckDBType.TimeNs, TimeSpan value) => NativeMethods.Value.DuckDBCreateTimeNs(DuckDBTimeNs.FromTicks(TimeOnly.FromTimeSpan(value).Ticks)),
            (DuckDBType.TimeTz, DateTimeOffset value) => NativeMethods.Value.DuckDBCreateTimeTz(value.ToTimeTzStruct()),
            (DuckDBType.Blob, byte[] value) => NativeMethods.Value.DuckDBCreateBlob(value, value.Length),
            (DuckDBType.List, ICollection value) => CreateCollectionValue(logicalType, value, true, dbType),
            (DuckDBType.Array, ICollection value) => CreateCollectionValue(logicalType, value, false, dbType),
            (_, ICollection value) when item is not byte[] => CreateListFromClrType(value, dbType),
            _ when ValueCreators.TryGetValue(dbType, out var converter) => converter(item),
            _ => InvariantText(item)
        };
    }

    // The value as its own type (an enum as its number), chosen by the value, not by the parameter's DbType.
    private static DuckDBValue ValueOfItsOwnType(object item)
    {
        if (item is Enum)
        {
            item = EnumNumber(item);
        }

        return ValueCreators.TryGetValue(DuckDBTypeMap.GetDbTypeForValue(item), out var creator) ? creator(item) : InvariantText(item);
    }

    // Not in the current culture, whose decimal comma or U+2212 minus sign DuckDB cannot parse.
    private static DuckDBValue InvariantText(object item)
    {
        var text = item is IFormattable formattable ? formattable.ToString(null, CultureInfo.InvariantCulture) : item.ToString();
        return CreateVarchar(text);
    }

    // An integer value, or an enum as its number, converted to T when it fits.
    private static bool TryConvertTo<T>(object item, out T result) where T : struct, IBinaryInteger<T>
    {
        switch (item)
        {
            case T value:
                result = value;
                return true;
            case sbyte value: return TryConvert(value, out result);
            case byte value: return TryConvert(value, out result);
            case short value: return TryConvert(value, out result);
            case ushort value: return TryConvert(value, out result);
            case int value: return TryConvert(value, out result);
            case uint value: return TryConvert(value, out result);
            case long value: return TryConvert(value, out result);
            case ulong value: return TryConvert(value, out result);
            case Int128 value: return TryConvert(value, out result);
            case UInt128 value: return TryConvert(value, out result);
            case BigInteger value: return TryConvert(value, out result);
            // Last: unlike the exact type tests above, the Enum test calls a cast helper.
            case Enum:
                return TryConvertTo(EnumNumber(item), out result);
            default:
                result = default;
                return false;
        }
    }

    // A boxed enum unboxes to its underlying type.
    private static object EnumNumber(object item)
    {
        return ((Enum)item).GetTypeCode() switch
        {
            TypeCode.SByte => (sbyte)item,
            TypeCode.Byte => (byte)item,
            TypeCode.Int16 => (short)item,
            TypeCode.UInt16 => (ushort)item,
            TypeCode.Int32 => (int)item,
            TypeCode.UInt32 => (uint)item,
            TypeCode.Int64 => (long)item,
            _ => (ulong)item,
        };
    }

    // CreateSaturating clamps to the range of T, so the value fits when converting back gives it again.
    private static bool TryConvert<TFrom, T>(TFrom value, out T result) where TFrom : IBinaryInteger<TFrom> where T : IBinaryInteger<T>
    {
        result = T.CreateSaturating(value);
        return TFrom.CreateSaturating(result) == value;
    }

    private static DuckDBValue CreateCollectionValue(DuckDBLogicalType logicalType, ICollection collection, bool isList, DbType dbType)
    {
        using var childType = isList ? NativeMethods.LogicalType.DuckDBListTypeChildType(logicalType) :
                                       NativeMethods.LogicalType.DuckDBArrayTypeChildType(logicalType);

        var values = BuildValues(childType, collection, dbType);

        var value = isList ? NativeMethods.Value.DuckDBCreateListValue(childType, values, collection.Count)
                           : NativeMethods.Value.DuckDBCreateArrayValue(childType, values, collection.Count);

        return ThrowIfNotCreated(value, values, childType, isList ? "list" : "array");
    }

    private static DuckDBValue CreateListFromClrType(ICollection collection, DbType dbType)
    {
        var elementType = collection.GetType().GetInterface(typeof(IEnumerable<>).Name)?.GetGenericArguments()[0];

        if (elementType == null)
        {
            return CreateVarchar(collection.ToString());
        }

        using var childType = elementType.GetLogicalType();
        var values = BuildValues(childType, collection, dbType);

        return ThrowIfNotCreated(NativeMethods.Value.DuckDBCreateListValue(childType, values, collection.Count), values, childType, "list");
    }

    // DuckDB converts every item to the type of the items of the list or array. When one cannot be converted, an
    // item out of range for example, it returns no value and does not say which item or why. Binding that would
    // crash the process: duckdb_bind_value reads the value without testing for it.
    private static DuckDBValue ThrowIfNotCreated(DuckDBValue value, DuckDBValue[] items, DuckDBLogicalType itemType, string kind)
    {
        if (!value.IsInvalid)
        {
            return value;
        }

        // A value that was not created does not own its items, so they are released here.
        foreach (var item in items)
        {
            item.Dispose();
        }

        value.Dispose();

        var itemTypeName = NativeMethods.LogicalType.DuckDBGetTypeId(itemType);

        throw new DuckDBException($"Cannot create the {kind} value of a parameter: DuckDB could not convert every item to {itemTypeName}, the type of its items. " +
                                  "An item may be out of range for that type.");
    }

    private static DuckDBValue[] BuildValues(DuckDBLogicalType childType, ICollection collection, DbType dbType)
    {
        var childDuckDBType = NativeMethods.LogicalType.DuckDBGetTypeId(childType);
        var values = new DuckDBValue[collection.Count];

        var index = 0;
        foreach (var item in collection)
        {
            values[index++] = item.ToDuckDBValue(childType, childDuckDBType, dbType);
        }

        return values;
    }

    // A BigInteger with no integer type to convert to: DuckDB reported no type for the parameter (an uncast "SELECT $1",
    // or a parameter cast to two different types), the value does not fit the integer parameter, or the parameter has
    // another type and the value goes through DbType.VarNumeric. Pick the smallest type that holds the value exactly:
    // HUGEINT, then UHUGEINT, then BIGNUM.
    private static DuckDBValue BigIntegerWithoutTargetType(BigInteger value)
    {
        if (value >= DuckDBHugeInt.HugeIntMinValue && value <= DuckDBHugeInt.HugeIntMaxValue)
        {
            return NativeMethods.Value.DuckDBCreateHugeInt(new DuckDBHugeInt(value));
        }

        if (value >= DuckDBUHugeInt.HugeIntMinValue && value <= DuckDBUHugeInt.HugeIntMaxValue)
        {
            return NativeMethods.Value.DuckDBCreateUHugeInt(new DuckDBUHugeInt(value));
        }

        return BigIntegerToDuckDBValue(value);
    }

    private static unsafe DuckDBValue BigIntegerToDuckDBValue(BigInteger value)
    {
        // A BIGNUM is built from the magnitude bytes and a sign, which keeps values of any size exact.
        // Going through HUGEINT instead would cap the value at 128 bits.
        var isNegative = value.Sign < 0;
        var magnitude = isNegative ? -value : value;
        var bytes = magnitude.ToByteArray(isUnsigned: true, isBigEndian: true);

        fixed (byte* data = bytes)
        {
            return NativeMethods.Value.DuckDBCreateBignum(new DuckDBBignum((IntPtr)data, (ulong)bytes.Length, isNegative ? (byte)1 : (byte)0));
        }
    }

    // Encoded here and passed as bytes, so a NUL character in the string is kept. Never null: ToDuckDBValue sends a
    // null value as NULL.
    private static DuckDBValue CreateVarchar(string? value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return NativeMethods.Value.DuckDBCreateVarchar(Encoding.UTF8.GetBytes(value));
    }

    // A decimal for a DECIMAL parameter goes with the parameter's width and scale when it fits them exactly, because
    // DuckDB binds the statement again for a value of another type, which makes an INSERT slower: a decimal's own
    // width and scale rarely match the column's. Otherwise it goes with its own, and DuckDB rounds the digits the
    // parameter has no room for, or refuses a value that is too large, as before.
    private static DuckDBValue DecimalToDuckDBValue(decimal value, DuckDBLogicalType parameterType)
    {
        var width = NativeMethods.LogicalType.DuckDBDecimalWidth(parameterType);
        var scale = NativeMethods.LogicalType.DuckDBDecimalScale(parameterType);

        value.GetParts(out var low, out var high, out var isNegative, out var valueScale);
        var magnitude = new UInt128(high, low);

        // Both results are below 10^width, so 10^38 at most: the multiplication cannot overflow the 128 bits.
        if (valueScale <= scale)
        {
            var shift = scale - valueScale;

            if (magnitude == 0 || DecimalExtensions.CountDigits(magnitude) + shift <= width)
            {
                return CreateDecimal(magnitude * (UInt128)DecimalExtensions.Int128PowersOfTen[shift], isNegative, width, scale);
            }
        }
        else
        {
            // More digits after the point than the parameter has: exact only when the extra ones are zeros (2.50m for a
            // scale of 1).
            var (quotient, remainder) = UInt128.DivRem(magnitude, (UInt128)DecimalExtensions.Int128PowersOfTen[valueScale - scale]);

            if (remainder == 0 && DecimalExtensions.CountDigits(quotient) <= width)
            {
                return CreateDecimal(quotient, isNegative, width, scale);
            }
        }

        return DecimalToDuckDBValue(value);
    }

    private static DuckDBValue CreateDecimal(UInt128 magnitude, bool isNegative, byte width, byte scale)
    {
        var integer = isNegative ? -(Int128)magnitude : (Int128)magnitude;
        return NativeMethods.Value.DuckDBCreateDecimal(new DuckDBDecimal(width, scale, new DuckDBHugeInt(integer)));
    }

    private static DuckDBValue DecimalToDuckDBValue(decimal value)
    {
        // The value goes to DuckDB as it is in the decimal: the integer, with the scale of the decimal. The writer of
        // the appender starts from the same parts (DecimalVectorDataWriter).
        value.GetParts(out var low, out var high, out var isNegative, out var scale);
        var magnitude = new UInt128(high, low);

        // The width is the number of digits. A value below one has a zero in front of its fraction.
        var width = Math.Max(DecimalExtensions.CountDigits(magnitude), scale + 1);

        var integer = isNegative ? -(Int128)magnitude : (Int128)magnitude;

        return NativeMethods.Value.DuckDBCreateDecimal(new DuckDBDecimal((byte)width, scale, new DuckDBHugeInt(integer)));
    }
}
