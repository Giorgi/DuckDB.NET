using System.Globalization;
using System.Text;
using DuckDB.NET.Data.Execution;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.PreparedStatement;

internal static class ClrToDuckDBConverter
{
    // The creator of each DbType DuckDBTypeMap gives for a value, called only with a value of that DbType's CLR type.
    private static readonly Dictionary<DbType, Func<IntPtr, object, DuckDBV2Value>> ValueCreators = new()
    {
        { DbType.Guid, (factory, value) => DuckDBV2Values.Uuid(factory, ((Guid)value).ToHugeInt()) },
        { DbType.Decimal, (factory, value) => DecimalToDuckDBValue(factory, (decimal)value) },
        { DbType.Boolean, (factory, value) => DuckDBV2Values.Bool(factory, (bool)value) },
        { DbType.SByte, (factory, value) => DuckDBV2Values.Int8(factory, (sbyte)value) },
        { DbType.Int16, (factory, value) => DuckDBV2Values.Int16(factory, (short)value) },
        { DbType.Int32, (factory, value) => DuckDBV2Values.Int32(factory, (int)value) },
        { DbType.Int64, (factory, value) => DuckDBV2Values.Int64(factory, (long)value) },
        { DbType.Byte, (factory, value) => DuckDBV2Values.UInt8(factory, (byte)value) },
        { DbType.UInt16, (factory, value) => DuckDBV2Values.UInt16(factory, (ushort)value) },
        { DbType.UInt32, (factory, value) => DuckDBV2Values.UInt32(factory, (uint)value) },
        { DbType.UInt64, (factory, value) => DuckDBV2Values.UInt64(factory, (ulong)value) },
        { DbType.Single, (factory, value) => DuckDBV2Values.Float(factory, (float)value) },
        { DbType.Double, (factory, value) => DuckDBV2Values.Double(factory, (double)value) },
        { DbType.String, (factory, value) => CreateVarchar(factory, (string?)value) },
        { DbType.VarNumeric, (factory, value) => value switch
            {
                Int128 int128 => DuckDBV2Values.HugeInt(factory, int128),
                UInt128 uint128 => DuckDBV2Values.UHugeInt(factory, uint128),
                _ => BigIntegerWithoutTargetType(factory, (BigInteger)value)
            }
        },
        { DbType.Binary, (factory, value) =>
            {
                return DuckDBV2Values.Blob(factory, (byte[])value);
            }
        },
        { DbType.Date, (factory, value) =>
            {
                var date = value is DateOnly dateOnly ? dateOnly.ToDuckDBDate() : ((DuckDBDateOnly)value).ToDuckDBDate();
                return DuckDBV2Values.Date(factory, date);
            }
        },
        { DbType.Time, (factory, value) =>
            {
                var time = value is TimeOnly timeOnly ? timeOnly.ToDuckDBTime() : ((DuckDBTimeOnly)value).ToDuckDBTime();
                return DuckDBV2Values.Time(factory, time);
            }
        },
        { DbType.DateTime, TimestampToDuckDBValue },
        { DbType.DateTimeOffset, (factory, value) => DuckDBV2Values.Timestamp(factory, ((DateTimeOffset)value).ToTimestampStruct(), DuckDBType.TimestampTz) },
    };

    // Creates the value bound to a parameter. duckDBType is the parameter's type as DuckDB reports it from the statement
    // (Invalid when it cannot tell, as in "SELECT $1"); dbType is the DbType the caller assigned, or Object when none
    // was. The first rule that applies:
    // 1. null or DBNull is a NULL.
    // 2. An assigned DbType converts the value to its CLR type when nothing is lost, or refuses it (DbTypeConverter);
    //    the rules below then see the converted value. A collection passes the DbType to each of its items.
    // 3. An integer parameter takes an integer or an enum that fits, converted to its type. Any other value, and one
    //    that does not fit, goes as its own type, or as text when it has none.
    // 4. Another parameter type takes a value of the matching .NET type as that type, and a TimeSpan with no reported
    //    type is an INTERVAL. A collection is built item by item, each item by these rules with the type of the items.
    // 5. Anything else goes as its own type, or as text formatted without the current culture.
    // Whatever does not have the parameter's type is cast by DuckDB, as it would cast a SQL literal: it rounds a
    // fraction and parses a string. A value that does not fit is refused on INSERT, matches no row in a comparison,
    // and fails to bind as a function argument. An enum is its number only for an integer parameter or an integer
    // DbType; elsewhere it goes as its name.
    // parameter names the parameter in the message of a refused value.
    // factory is the connection's, logicalType the parameter's (borrowed; zero when there is no type to give).
    public static DuckDBV2Value ToDuckDBValue(this object? item, IntPtr factory, IntPtr logicalType, DuckDBType duckDBType, DbType dbType, ParameterLabel parameter = default)
    {
        if (item.IsNull())
        {
            return DuckDBV2Values.Null(factory);
        }

        // A byte[] is one value, except for a LIST or ARRAY parameter, which takes its bytes as items.
        var isCollection = item is ICollection && (item is not byte[] || duckDBType is DuckDBType.List or DuckDBType.Array);

        if (dbType != DbType.Object && !isCollection)
        {
            item = DbTypeConverter.Convert(item, dbType, duckDBType, parameter);
        }
        else if (dbType == DbType.Binary && item is byte[])
        {
            // DbType.Binary describes the byte[] as a whole, not its bytes.
            dbType = DbType.Object;
        }

        return (duckDBType, item) switch
        {
            (DuckDBType.Boolean, bool value) => DuckDBV2Values.Bool(factory, value),

            // An integer that fits is converted here, for two reasons. DuckDB binds the statement again when the value's
            // type differs from the parameter's, which makes an INSERT slower. And some function arguments take only
            // the exact type: lpad('2', $1, '0') reports INTEGER and does not bind with a BIGINT value, so a long works
            // there only because it is narrowed. Anything else goes as its own type (or as text), and DuckDB casts it
            // like a SQL literal.
            (DuckDBType.TinyInt, _) => TryConvertTo<sbyte>(item, out var result) ? DuckDBV2Values.Int8(factory, result) : ValueOfItsOwnType(factory, item),
            (DuckDBType.SmallInt, _) => TryConvertTo<short>(item, out var result) ? DuckDBV2Values.Int16(factory, result) : ValueOfItsOwnType(factory, item),
            (DuckDBType.Integer, _) => TryConvertTo<int>(item, out var result) ? DuckDBV2Values.Int32(factory, result) : ValueOfItsOwnType(factory, item),
            (DuckDBType.BigInt, _) => TryConvertTo<long>(item, out var result) ? DuckDBV2Values.Int64(factory, result) : ValueOfItsOwnType(factory, item),
            (DuckDBType.HugeInt, _) => TryConvertTo<Int128>(item, out var result) ? DuckDBV2Values.HugeInt(factory, result) : ValueOfItsOwnType(factory, item),

            (DuckDBType.UnsignedTinyInt, _) => TryConvertTo<byte>(item, out var result) ? DuckDBV2Values.UInt8(factory, result) : ValueOfItsOwnType(factory, item),
            (DuckDBType.UnsignedSmallInt, _) => TryConvertTo<ushort>(item, out var result) ? DuckDBV2Values.UInt16(factory, result) : ValueOfItsOwnType(factory, item),
            (DuckDBType.UnsignedInteger, _) => TryConvertTo<uint>(item, out var result) ? DuckDBV2Values.UInt32(factory, result) : ValueOfItsOwnType(factory, item),
            (DuckDBType.UnsignedBigInt, _) => TryConvertTo<ulong>(item, out var result) ? DuckDBV2Values.UInt64(factory, result) : ValueOfItsOwnType(factory, item),
            (DuckDBType.UnsignedHugeInt, _) => TryConvertTo<UInt128>(item, out var result) ? DuckDBV2Values.UHugeInt(factory, result) : ValueOfItsOwnType(factory, item),

            (DuckDBType.Float, float value) => DuckDBV2Values.Float(factory, value),
            (DuckDBType.Double, double value) => DuckDBV2Values.Double(factory, value),

            (DuckDBType.Decimal, decimal value) => DecimalToDuckDBValue(factory, value, logicalType),
            (DuckDBType.VarInt, BigInteger value) => DuckDBV2Values.Bignum(factory, value),

            (DuckDBType.Varchar, string value) => CreateVarchar(factory, value),
            (DuckDBType.Uuid, Guid value) => DuckDBV2Values.Uuid(factory, value.ToHugeInt()),

            (DuckDBType.Timestamp, DateTime value) => DuckDBV2Values.Timestamp(factory, value.ToTimestampStruct(duckDBType), DuckDBType.Timestamp),
            (DuckDBType.TimestampS, DateTime value) => DuckDBV2Values.Timestamp(factory, value.ToTimestampStruct(duckDBType), DuckDBType.TimestampS),
            (DuckDBType.TimestampMs, DateTime value) => DuckDBV2Values.Timestamp(factory, value.ToTimestampStruct(duckDBType), DuckDBType.TimestampMs),
            (DuckDBType.TimestampNs, DateTime value) => DuckDBV2Values.Timestamp(factory, value.ToTimestampStruct(duckDBType), DuckDBType.TimestampNs),
            (DuckDBType.TimestampTz, DateTime value) => DuckDBV2Values.Timestamp(factory, value.ToTimestampStruct(duckDBType), DuckDBType.TimestampTz),
            (DuckDBType.TimestampTz, DateTimeOffset value) => DuckDBV2Values.Timestamp(factory, value.ToTimestampStruct(), DuckDBType.TimestampTz),
            (DuckDBType.Interval, TimeSpan value) => DuckDBV2Values.Interval(factory, value),
            // No inferred type (a bare "?"): TimeSpan has no DbType, and its ToString() isn't interval text
            // DuckDB can parse once there are days ("1.01:01:01"). DbType.Time has made it a TimeOnly already.
            (DuckDBType.Invalid, TimeSpan value) => DuckDBV2Values.Interval(factory, value),
            (DuckDBType.Date, DateTime value) => DuckDBV2Values.Date(factory, value.ToDuckDBDate()),
            (DuckDBType.Date, DuckDBDateOnly value) => DuckDBV2Values.Date(factory, value.ToDuckDBDate()),
            (DuckDBType.Time, DateTime value) => DuckDBV2Values.Time(factory, value.ToDuckDBTime()),
            (DuckDBType.Time, DuckDBTimeOnly value) => DuckDBV2Values.Time(factory, value.ToDuckDBTime()),
            (DuckDBType.Date, DateOnly value) => DuckDBV2Values.Date(factory, value.ToDuckDBDate()),
            (DuckDBType.Time, TimeOnly value) => DuckDBV2Values.Time(factory, value.ToDuckDBTime()),
            (DuckDBType.TimeNs, DateTime value) => DuckDBV2Values.TimeNs(factory, DuckDBTimeNs.FromTicks(value.Ticks % TimeSpan.TicksPerDay)),
            (DuckDBType.TimeNs, DuckDBTimeOnly value) => DuckDBV2Values.TimeNs(factory, DuckDBTimeNs.FromTicks(value.Ticks)),
            (DuckDBType.TimeNs, TimeOnly value) => DuckDBV2Values.TimeNs(factory, DuckDBTimeNs.FromTicks(value.Ticks)),
            // A TimeSpan is a time of day only from zero to a day; any other goes as text, which DuckDB refuses for a TIME_NS.
            (DuckDBType.TimeNs, TimeSpan value) when value >= TimeSpan.Zero && value < TimeSpan.FromDays(1) =>DuckDBV2Values.TimeNs(factory, DuckDBTimeNs.FromTicks(TimeOnly.FromTimeSpan(value).Ticks)),
            (DuckDBType.TimeTz, DateTimeOffset value) => DuckDBV2Values.TimeTz(factory, value.ToTimeTzStruct()),
            (DuckDBType.Blob, byte[] value) => DuckDBV2Values.Blob(factory, value),
            (DuckDBType.List, ICollection value) => CreateCollectionValue(factory, logicalType, value, true, dbType, parameter),
            (DuckDBType.Array, ICollection value) => CreateCollectionValue(factory, logicalType, value, false, dbType, parameter),
            (_, ICollection value) when item is not byte[] => CreateListFromClrType(factory, value, dbType, parameter),
            _ => ValueOfItsDbType(factory, item)
        };
    }

    // The value as its own type (an enum as its number), chosen by the value, not by the parameter's DbType.
    private static DuckDBV2Value ValueOfItsOwnType(IntPtr factory, object item)
    {
        return ValueOfItsDbType(factory, item is Enum ? EnumNumber(item) : item);
    }

    // The creator of the DbType DuckDBTypeMap gives for the value, or text when it has none (an enum: its name).
    private static DuckDBV2Value ValueOfItsDbType(IntPtr factory, object item)
    {
        return ValueCreators.TryGetValue(DuckDBTypeMap.GetDbTypeForValue(item), out var creator) ? creator(factory, item) : InvariantText(factory, item);
    }

    // Not in the current culture, whose decimal comma or U+2212 minus sign DuckDB cannot parse.
    private static DuckDBV2Value InvariantText(IntPtr factory, object item)
    {
        var text = item is IFormattable formattable ? formattable.ToString(null, CultureInfo.InvariantCulture) : item.ToString();
        return CreateVarchar(factory, text);
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
    internal static object EnumNumber(object item)
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

    private static unsafe DuckDBV2Value CreateCollectionValue(IntPtr factory, IntPtr logicalType, ICollection collection, bool isList, DbType dbType, ParameterLabel parameter)
    {
        using var itemType = DuckDBV2LogicalTypes.GetTypeParam((_duckdb_v2_logical_type*)logicalType, 0, out _);
        return CreateCollection(factory, itemType, collection, isList, dbType, parameter);
    }

    private static DuckDBV2Value CreateListFromClrType(IntPtr factory, ICollection collection, DbType dbType, ParameterLabel parameter)
    {
        var elementType = collection.GetType().GetInterface(typeof(IEnumerable<>).Name)?.GetGenericArguments()[0];

        // With an assigned DbType the items are converted to its CLR type, so the list has that type's items. Not when
        // the items are collections themselves, and not for Decimal and VarNumeric when the element type is a concrete
        // one: their CLR types map to a fixed DECIMAL(38,18) and HUGEINT, which a converted value can exceed, where the
        // element type keeps it. With no element type, or object, there is nothing to keep.
        var itemsAreCollections = elementType is not null && elementType != typeof(string) && elementType != typeof(byte[]) &&
                                  typeof(IEnumerable).IsAssignableFrom(elementType);
        var keepsElementType = elementType is not null && elementType != typeof(object) &&
                               dbType is DbType.Decimal or DbType.Currency or DbType.VarNumeric;

        if (!itemsAreCollections && !keepsElementType && DuckDBTypeMap.GetClrType(dbType) is { } dbTypeClrType)
        {
            elementType = dbTypeClrType;
        }

        if (elementType == null)
        {
            return CreateVarchar(factory, collection.ToString());
        }

        using var itemType = DuckDBV2Types.ForClrType(factory, elementType);
        return CreateCollection(factory, itemType, collection, true, dbType, parameter);
    }

    // DuckDB converts every item to the type of the items of the list or array. When one cannot be converted, an item
    // out of range for example, V2 reports the error. The items are copied into the list, so they are released here
    // whether or not it was created.
    private static unsafe DuckDBV2Value CreateCollection(IntPtr factory, DuckDBV2LogicalType itemType, ICollection collection, bool isList, DbType dbType, ParameterLabel parameter)
    {
        using var lease = itemType.Acquire();
        var itemTypePointer = (IntPtr)lease.Pointer;

        var items = BuildValues(factory, itemTypePointer, collection, dbType, parameter);

        try
        {
            return DuckDBV2Values.Collection(factory, itemTypePointer, items, isList);
        }
        catch (DuckDBException exception)
        {
            var kind = isList ? "list" : "array";
            var itemTypeName = DuckDBV2LogicalTypes.GetId(lease.Pointer).ToDuckDBType();

            throw new DuckDBException($"Cannot create the {kind} value of a parameter: DuckDB could not convert every item to {itemTypeName}, the type of its items. " +
                                      $"An item may be out of range for that type. {exception.Message}", exception);
        }
        finally
        {
            foreach (var item in items)
            {
                item.Dispose();
            }
        }
    }

    private static unsafe DuckDBV2Value[] BuildValues(IntPtr factory, IntPtr itemType, ICollection collection, DbType dbType, ParameterLabel parameter)
    {
        var itemDuckDBType = DuckDBV2LogicalTypes.GetId((_duckdb_v2_logical_type*)itemType).ToDuckDBType();
        var values = new DuckDBV2Value[collection.Count];

        var index = 0;
        try
        {
            foreach (var item in collection)
            {
                values[index] = item.ToDuckDBValue(factory, itemType, itemDuckDBType, dbType, parameter);
                index++;
            }
        }
        catch
        {
            // An item was refused: release the items created before it, which nothing owns yet.
            for (var created = 0; created < index; created++)
            {
                values[created].Dispose();
            }

            throw;
        }

        return values;
    }

    // A BigInteger with no integer type to convert to: DuckDB reported no type for the parameter (an uncast "SELECT $1",
    // or a parameter cast to two different types), the value does not fit the integer parameter, or the parameter has
    // another type and the value goes through DbType.VarNumeric. Pick the smallest type that holds the value exactly:
    // HUGEINT, then UHUGEINT, then BIGNUM.
    private static DuckDBV2Value BigIntegerWithoutTargetType(IntPtr factory, BigInteger value)
    {
        if (value >= DuckDBHugeInt.HugeIntMinValue && value <= DuckDBHugeInt.HugeIntMaxValue)
        {
            return DuckDBV2Values.HugeInt(factory, (Int128)value);
        }

        if (value >= DuckDBUHugeInt.HugeIntMinValue && value <= DuckDBUHugeInt.HugeIntMaxValue)
        {
            return DuckDBV2Values.UHugeInt(factory, (UInt128)value);
        }

        // A BIGNUM keeps values of any size exact; going through HUGEINT instead would cap the value at 128 bits.
        return DuckDBV2Values.Bignum(factory, value);
    }

    // Encoded here and passed as bytes, so a NUL character in the string is kept. Never null: ToDuckDBValue sends a
    // null value as NULL.
    private static DuckDBV2Value CreateVarchar(IntPtr factory, string? value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return DuckDBV2Values.Varchar(factory, value);
    }

    // A decimal for a DECIMAL parameter goes with the parameter's width and scale when it fits them exactly, because
    // DuckDB binds the statement again for a value of another type, which makes an INSERT slower: a decimal's own
    // width and scale rarely match the column's. Otherwise it goes with its own, and DuckDB rounds the digits the
    // parameter has no room for, or refuses a value that is too large, as before.
    private static unsafe DuckDBV2Value DecimalToDuckDBValue(IntPtr factory, decimal value, IntPtr parameterType)
    {
        var width = (byte)DuckDBV2LogicalTypes.GetUIntParam((_duckdb_v2_logical_type*)parameterType, 0);
        var scale = (byte)DuckDBV2LogicalTypes.GetUIntParam((_duckdb_v2_logical_type*)parameterType, 1);

        value.GetParts(out var low, out var high, out var isNegative, out var valueScale);
        var magnitude = new UInt128(high, low);

        if (valueScale <= scale)
        {
            var shift = scale - valueScale;

            // The product has fewer digits than the width, 38 at most, so it cannot overflow the 128 bits.
            if (magnitude == 0 || DecimalExtensions.CountDigits(magnitude) + shift <= width)
            {
                return CreateDecimal(factory, magnitude * (UInt128)DecimalExtensions.Int128PowersOfTen[shift], isNegative, width, scale);
            }
        }
        else
        {
            // More digits after the point than the parameter has: exact only when the extra ones are zeros (2.50m for a
            // scale of 1).
            var (quotient, remainder) = UInt128.DivRem(magnitude, (UInt128)DecimalExtensions.Int128PowersOfTen[valueScale - scale]);

            if (remainder == 0 && DecimalExtensions.CountDigits(quotient) <= width)
            {
                return CreateDecimal(factory, quotient, isNegative, width, scale);
            }
        }

        return DecimalToDuckDBValue(factory, value);
    }

    private static DuckDBV2Value CreateDecimal(IntPtr factory, UInt128 magnitude, bool isNegative, byte width, byte scale)
    {
        var integer = isNegative ? -(Int128)magnitude : (Int128)magnitude;
        return DuckDBV2Values.Decimal(factory, integer, width, scale);
    }

    private static DuckDBV2Value TimestampToDuckDBValue(IntPtr factory, object value)
    {
        var dateTime = value is DateTime dt ? dt.ToTimestampStruct(DuckDBType.Timestamp) : ((DuckDBTimestamp)value).ToDuckDBTimestampStruct();
        return DuckDBV2Values.Timestamp(factory, dateTime, DuckDBType.Timestamp);
    }

    private static DuckDBV2Value DecimalToDuckDBValue(IntPtr factory, decimal value)
    {
        // The value goes to DuckDB as it is in the decimal: the integer, with the scale of the decimal. The writer of
        // the appender starts from the same parts (DecimalVectorDataWriter).
        value.GetParts(out var low, out var high, out var isNegative, out var scale);
        var magnitude = new UInt128(high, low);

        // The width is the number of digits. A value below one has a zero in front of its fraction.
        var width = Math.Max(DecimalExtensions.CountDigits(magnitude), scale + 1);

        var integer = isNegative ? -(Int128)magnitude : (Int128)magnitude;

        return DuckDBV2Values.Decimal(factory, integer, (byte)width, scale);
    }
}
