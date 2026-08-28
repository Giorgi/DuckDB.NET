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
        { DbType.String, value => NativeMethods.Value.DuckDBCreateVarchar((string?)value) },
        { DbType.VarNumeric, value => NativeMethods.Value.DuckDBCreateHugeInt(new((BigInteger)value)) },
        { DbType.Binary, value =>
            {
                var bytes = (byte[])value;
                return NativeMethods.Value.DuckDBCreateBlob(bytes, bytes.Length);
            }
        },
        { DbType.Date, value =>
            {
                var date = (value is DateOnly dateOnly ? (DuckDBDateOnly)dateOnly : (DuckDBDateOnly)value).ToDuckDBDate();
                return NativeMethods.Value.DuckDBCreateDate(date);
            }
        },
        { DbType.Time, value =>
            {
                var time = NativeMethods.DateTimeHelpers.DuckDBToTime(value is TimeOnly timeOnly ? (DuckDBTimeOnly)timeOnly : (DuckDBTimeOnly)value);
                return NativeMethods.Value.DuckDBCreateTime(time);
            }
        },
        { DbType.DateTime, value =>
            {
                var dateTime = (value is DateTime dt ? (DuckDBTimestamp)dt : (DuckDBTimestamp)value).ToDuckDBTimestampStruct();
                return NativeMethods.Value.DuckDBCreateTimestamp(dateTime);
            }
        },
        { DbType.DateTimeOffset, value => NativeMethods.Value.DuckDBCreateTimestampTz(((DateTimeOffset)value).ToTimestampStruct()) },
    };

    public static DuckDBValue ToDuckDBValue(this object? item, DuckDBLogicalType logicalType, DuckDBType duckDBType, DbType dbType)
    {
        if (item.IsNull())
        {
            return NativeMethods.Value.DuckDBCreateNullValue();
        }

        return (duckDBType, item) switch
        {
            (DuckDBType.Boolean, bool value) => NativeMethods.Value.DuckDBCreateBool(value),

            (DuckDBType.TinyInt, _) => TryConvertTo(item, out sbyte result) ? NativeMethods.Value.DuckDBCreateInt8(result) : NativeMethods.Value.DuckDBCreateVarchar(item.ToString()),
            (DuckDBType.SmallInt, _) => TryConvertTo(item, out short result) ? NativeMethods.Value.DuckDBCreateInt16(result) : NativeMethods.Value.DuckDBCreateVarchar(item.ToString()),
            (DuckDBType.Integer, _) => TryConvertTo(item, out int result) ? NativeMethods.Value.DuckDBCreateInt32(result) : NativeMethods.Value.DuckDBCreateVarchar(item.ToString()),
            (DuckDBType.BigInt, _) => TryConvertTo(item, out long result) ? NativeMethods.Value.DuckDBCreateInt64(result) : NativeMethods.Value.DuckDBCreateVarchar(item.ToString()),

            (DuckDBType.UnsignedTinyInt, _) => TryConvertTo(item, out byte result) ? NativeMethods.Value.DuckDBCreateUInt8(result) : NativeMethods.Value.DuckDBCreateVarchar(item.ToString()),
            (DuckDBType.UnsignedSmallInt, _) => TryConvertTo(item, out ushort result) ? NativeMethods.Value.DuckDBCreateUInt16(result) : NativeMethods.Value.DuckDBCreateVarchar(item.ToString()),
            (DuckDBType.UnsignedInteger, _) => TryConvertTo(item, out uint result) ? NativeMethods.Value.DuckDBCreateUInt32(result) : NativeMethods.Value.DuckDBCreateVarchar(item.ToString()),
            (DuckDBType.UnsignedBigInt, _) => TryConvertTo(item, out ulong result) ? NativeMethods.Value.DuckDBCreateUInt64(result) : NativeMethods.Value.DuckDBCreateVarchar(item.ToString()),

            (DuckDBType.Float, float value) => NativeMethods.Value.DuckDBCreateFloat(value),
            (DuckDBType.Double, double value) => NativeMethods.Value.DuckDBCreateDouble(value),

            (DuckDBType.Decimal, decimal value) => DecimalToDuckDBValue(value),
            (DuckDBType.HugeInt, BigInteger value) => NativeMethods.Value.DuckDBCreateHugeInt(new DuckDBHugeInt(value)),

            (DuckDBType.Varchar, string value) => NativeMethods.Value.DuckDBCreateVarchar(value),
            (DuckDBType.Uuid, Guid value) => NativeMethods.Value.DuckDBCreateUuid(value.ToHugeInt(false)),

            (DuckDBType.Timestamp, DateTime value) => NativeMethods.Value.DuckDBCreateTimestamp(value.ToTimestampStruct(duckDBType)),
            (DuckDBType.TimestampS, DateTime value) => NativeMethods.Value.DuckDBCreateTimestampS(value.ToTimestampStruct(duckDBType)),
            (DuckDBType.TimestampMs, DateTime value) => NativeMethods.Value.DuckDBCreateTimestampMs(value.ToTimestampStruct(duckDBType)),
            (DuckDBType.TimestampNs, DateTime value) => NativeMethods.Value.DuckDBCreateTimestampNs(value.ToTimestampStruct(duckDBType)),
            (DuckDBType.TimestampTz, DateTime value) => NativeMethods.Value.DuckDBCreateTimestampTz(value.ToTimestampStruct(duckDBType)),
            (DuckDBType.TimestampTz, DateTimeOffset value) => NativeMethods.Value.DuckDBCreateTimestampTz(value.ToTimestampStruct()),
            (DuckDBType.Interval, TimeSpan value) => NativeMethods.Value.DuckDBCreateInterval(value),
            (DuckDBType.Date, DateTime value) => NativeMethods.Value.DuckDBCreateDate(((DuckDBDateOnly)value).ToDuckDBDate()),
            (DuckDBType.Date, DuckDBDateOnly value) => NativeMethods.Value.DuckDBCreateDate(value.ToDuckDBDate()),
            (DuckDBType.Time, DateTime value) => NativeMethods.Value.DuckDBCreateTime(NativeMethods.DateTimeHelpers.DuckDBToTime((DuckDBTimeOnly)value)),
            (DuckDBType.Time, DuckDBTimeOnly value) => NativeMethods.Value.DuckDBCreateTime(NativeMethods.DateTimeHelpers.DuckDBToTime(value)),
            (DuckDBType.Date, DateOnly value) => NativeMethods.Value.DuckDBCreateDate(((DuckDBDateOnly)value).ToDuckDBDate()),
            (DuckDBType.Time, TimeOnly value) => NativeMethods.Value.DuckDBCreateTime(NativeMethods.DateTimeHelpers.DuckDBToTime(value)),
            (DuckDBType.TimeTz, DateTimeOffset value) => NativeMethods.Value.DuckDBCreateTimeTz(value.ToTimeTzStruct()),
            (DuckDBType.Blob, byte[] value) => NativeMethods.Value.DuckDBCreateBlob(value, value.Length),
            (DuckDBType.List, ICollection value) => CreateCollectionValue(logicalType, value, true, dbType),
            (DuckDBType.Array, ICollection value) => CreateCollectionValue(logicalType, value, false, dbType),
            (_, ICollection value) when item is not byte[] => CreateListFromClrType(value, dbType),
            _ when ValueCreators.TryGetValue(dbType, out var converter) => converter(item),
            _ => NativeMethods.Value.DuckDBCreateVarchar(item.ToString())
        };
    }

    public static bool TryBindScalarValue(this object? item, DuckDBPreparedStatement statement, long index, DuckDBType duckDBType, DbType dbType, out DuckDBState result)
    {
        if (item.IsNull())
        {
            result = NativeMethods.PreparedStatements.DuckDBBindNull(statement, index);
            return true;
        }

        switch (duckDBType, item)
        {
            case (DuckDBType.Boolean, bool booleanValue):
                result = NativeMethods.PreparedStatements.DuckDBBindBoolean(statement, index, booleanValue);
                return true;

            case (DuckDBType.TinyInt, _):
                result = TryConvertTo(item, out sbyte int8Value)
                    ? NativeMethods.PreparedStatements.DuckDBBindInt8(statement, index, int8Value)
                    : NativeMethods.PreparedStatements.DuckDBBindVarchar(statement, index, item.ToString()!);
                return true;
            case (DuckDBType.SmallInt, _):
                result = TryConvertTo(item, out short int16Value)
                    ? NativeMethods.PreparedStatements.DuckDBBindInt16(statement, index, int16Value)
                    : NativeMethods.PreparedStatements.DuckDBBindVarchar(statement, index, item.ToString()!);
                return true;
            case (DuckDBType.Integer, _):
                result = TryConvertTo(item, out int int32Value)
                    ? NativeMethods.PreparedStatements.DuckDBBindInt32(statement, index, int32Value)
                    : NativeMethods.PreparedStatements.DuckDBBindVarchar(statement, index, item.ToString()!);
                return true;
            case (DuckDBType.BigInt, _):
                result = TryConvertTo(item, out long int64Value)
                    ? NativeMethods.PreparedStatements.DuckDBBindInt64(statement, index, int64Value)
                    : NativeMethods.PreparedStatements.DuckDBBindVarchar(statement, index, item.ToString()!);
                return true;

            case (DuckDBType.UnsignedTinyInt, _):
                result = TryConvertTo(item, out byte uint8Value)
                    ? NativeMethods.PreparedStatements.DuckDBBindUInt8(statement, index, uint8Value)
                    : NativeMethods.PreparedStatements.DuckDBBindVarchar(statement, index, item.ToString()!);
                return true;
            case (DuckDBType.UnsignedSmallInt, _):
                result = TryConvertTo(item, out ushort uint16Value)
                    ? NativeMethods.PreparedStatements.DuckDBBindUInt16(statement, index, uint16Value)
                    : NativeMethods.PreparedStatements.DuckDBBindVarchar(statement, index, item.ToString()!);
                return true;
            case (DuckDBType.UnsignedInteger, _):
                result = TryConvertTo(item, out uint uint32Value)
                    ? NativeMethods.PreparedStatements.DuckDBBindUInt32(statement, index, uint32Value)
                    : NativeMethods.PreparedStatements.DuckDBBindVarchar(statement, index, item.ToString()!);
                return true;
            case (DuckDBType.UnsignedBigInt, _):
                result = TryConvertTo(item, out ulong uint64Value)
                    ? NativeMethods.PreparedStatements.DuckDBBindUInt64(statement, index, uint64Value)
                    : NativeMethods.PreparedStatements.DuckDBBindVarchar(statement, index, item.ToString()!);
                return true;

            case (DuckDBType.Float, float floatValue):
                result = NativeMethods.PreparedStatements.DuckDBBindFloat(statement, index, floatValue);
                return true;
            case (DuckDBType.Double, double doubleValue):
                result = NativeMethods.PreparedStatements.DuckDBBindDouble(statement, index, doubleValue);
                return true;
            case (DuckDBType.Decimal, decimal decimalValue):
                result = NativeMethods.PreparedStatements.DuckDBBindDecimal(statement, index, ToDuckDBDecimal(decimalValue));
                return true;
            case (DuckDBType.HugeInt, BigInteger hugeIntValue):
                result = NativeMethods.PreparedStatements.DuckDBBindHugeInt(statement, index, new DuckDBHugeInt(hugeIntValue));
                return true;

            case (DuckDBType.Varchar, string stringValue):
                result = NativeMethods.PreparedStatements.DuckDBBindVarchar(statement, index, stringValue);
                return true;
            case (DuckDBType.Timestamp, DateTime timestampValue):
                result = NativeMethods.PreparedStatements.DuckDBBindTimestamp(statement, index, timestampValue.ToTimestampStruct(duckDBType));
                return true;
            case (DuckDBType.TimestampTz, DateTime timestampTzValue):
                result = NativeMethods.PreparedStatements.DuckDBBindTimestampTz(statement, index, timestampTzValue.ToTimestampStruct(duckDBType));
                return true;
            case (DuckDBType.TimestampTz, DateTimeOffset timestampOffsetValue):
                result = NativeMethods.PreparedStatements.DuckDBBindTimestampTz(statement, index, timestampOffsetValue.ToTimestampStruct());
                return true;
            case (DuckDBType.Interval, TimeSpan intervalValue):
                result = NativeMethods.PreparedStatements.DuckDBBindInterval(statement, index, intervalValue);
                return true;
            case (DuckDBType.Date, DateTime dateTimeValue):
                result = NativeMethods.PreparedStatements.DuckDBBindDate(statement, index, ((DuckDBDateOnly)dateTimeValue).ToDuckDBDate());
                return true;
            case (DuckDBType.Date, DuckDBDateOnly duckDBDateValue):
                result = NativeMethods.PreparedStatements.DuckDBBindDate(statement, index, duckDBDateValue.ToDuckDBDate());
                return true;
            case (DuckDBType.Date, DateOnly dateOnlyValue):
                result = NativeMethods.PreparedStatements.DuckDBBindDate(statement, index, ((DuckDBDateOnly)dateOnlyValue).ToDuckDBDate());
                return true;
            case (DuckDBType.Time, DateTime timeDateTimeValue):
                result = NativeMethods.PreparedStatements.DuckDBBindTime(statement, index, NativeMethods.DateTimeHelpers.DuckDBToTime((DuckDBTimeOnly)timeDateTimeValue));
                return true;
            case (DuckDBType.Time, DuckDBTimeOnly duckDBTimeValue):
                result = NativeMethods.PreparedStatements.DuckDBBindTime(statement, index, NativeMethods.DateTimeHelpers.DuckDBToTime(duckDBTimeValue));
                return true;
            case (DuckDBType.Time, TimeOnly timeOnlyValue):
                result = NativeMethods.PreparedStatements.DuckDBBindTime(statement, index, NativeMethods.DateTimeHelpers.DuckDBToTime(timeOnlyValue));
                return true;
            case (DuckDBType.Blob, byte[] blobValue):
                result = NativeMethods.PreparedStatements.DuckDBBindBlob(statement, index, blobValue, blobValue.LongLength);
                return true;
        }

        // These logical types need the exact duckdb_value representation because the scalar C API
        // has no equivalent binder, or because its timestamp unit differs from duckdb_bind_timestamp.
        if (item is Guid ||
            item is DateTime && duckDBType is DuckDBType.TimestampS or DuckDBType.TimestampMs or DuckDBType.TimestampNs ||
            item is DateTimeOffset && duckDBType == DuckDBType.TimeTz ||
            item is ICollection && (item is not byte[] || duckDBType is DuckDBType.List or DuckDBType.Array))
        {
            result = default;
            return false;
        }

        switch (dbType)
        {
            case DbType.Currency:
                result = NativeMethods.PreparedStatements.DuckDBBindDecimal(statement, index, ToDuckDBDecimal((decimal)item));
                return true;
            case DbType.Boolean:
                result = NativeMethods.PreparedStatements.DuckDBBindBoolean(statement, index, (bool)item);
                return true;
            case DbType.SByte:
                result = NativeMethods.PreparedStatements.DuckDBBindInt8(statement, index, (sbyte)item);
                return true;
            case DbType.Int16:
                result = NativeMethods.PreparedStatements.DuckDBBindInt16(statement, index, (short)item);
                return true;
            case DbType.Int32:
                result = NativeMethods.PreparedStatements.DuckDBBindInt32(statement, index, (int)item);
                return true;
            case DbType.Int64:
                result = NativeMethods.PreparedStatements.DuckDBBindInt64(statement, index, (long)item);
                return true;
            case DbType.Byte:
                result = NativeMethods.PreparedStatements.DuckDBBindUInt8(statement, index, (byte)item);
                return true;
            case DbType.UInt16:
                result = NativeMethods.PreparedStatements.DuckDBBindUInt16(statement, index, (ushort)item);
                return true;
            case DbType.UInt32:
                result = NativeMethods.PreparedStatements.DuckDBBindUInt32(statement, index, (uint)item);
                return true;
            case DbType.UInt64:
                result = NativeMethods.PreparedStatements.DuckDBBindUInt64(statement, index, (ulong)item);
                return true;
            case DbType.Single:
                result = NativeMethods.PreparedStatements.DuckDBBindFloat(statement, index, (float)item);
                return true;
            case DbType.Double:
                result = NativeMethods.PreparedStatements.DuckDBBindDouble(statement, index, (double)item);
                return true;
            case DbType.String:
                result = NativeMethods.PreparedStatements.DuckDBBindVarchar(statement, index, (string)item);
                return true;
            case DbType.VarNumeric:
                result = NativeMethods.PreparedStatements.DuckDBBindHugeInt(statement, index, new DuckDBHugeInt((BigInteger)item));
                return true;
            case DbType.Binary:
                var bytes = (byte[])item;
                result = NativeMethods.PreparedStatements.DuckDBBindBlob(statement, index, bytes, bytes.LongLength);
                return true;
            case DbType.Date:
                var date = (item is DateOnly dateOnly ? (DuckDBDateOnly)dateOnly : (DuckDBDateOnly)item).ToDuckDBDate();
                result = NativeMethods.PreparedStatements.DuckDBBindDate(statement, index, date);
                return true;
            case DbType.Time:
                var time = NativeMethods.DateTimeHelpers.DuckDBToTime(item is TimeOnly timeOnly ? (DuckDBTimeOnly)timeOnly : (DuckDBTimeOnly)item);
                result = NativeMethods.PreparedStatements.DuckDBBindTime(statement, index, time);
                return true;
            case DbType.DateTime:
                var timestamp = (item is DateTime dateTime ? (DuckDBTimestamp)dateTime : (DuckDBTimestamp)item).ToDuckDBTimestampStruct();
                result = NativeMethods.PreparedStatements.DuckDBBindTimestamp(statement, index, timestamp);
                return true;
            case DbType.DateTimeOffset:
                result = NativeMethods.PreparedStatements.DuckDBBindTimestampTz(statement, index, ((DateTimeOffset)item).ToTimestampStruct());
                return true;
            default:
                result = default;
                return false;
        }
    }

    private static bool TryConvertTo<T>(object item, out T result) where T : struct
    {
        try
        {
            if (item is T parsable)
            {
                result = parsable;
                return true;
            }

            result = (T)Convert.ChangeType(item, typeof(T));
            return true;
        }
        catch (Exception)
        {
            result = default;
            return false;
        }
    }

    private static DuckDBValue CreateCollectionValue(DuckDBLogicalType logicalType, ICollection collection, bool isList, DbType dbType)
    {
        using var childType = isList ? NativeMethods.LogicalType.DuckDBListTypeChildType(logicalType) :
                                       NativeMethods.LogicalType.DuckDBArrayTypeChildType(logicalType);

        var values = BuildValues(childType, collection, dbType);

        return isList ? NativeMethods.Value.DuckDBCreateListValue(childType, values, collection.Count)
                      : NativeMethods.Value.DuckDBCreateArrayValue(childType, values, collection.Count);
    }

    private static DuckDBValue CreateListFromClrType(ICollection collection, DbType dbType)
    {
        var elementType = collection.GetType().GetInterface(typeof(IEnumerable<>).Name)?.GetGenericArguments()[0];

        if (elementType == null)
        {
            return NativeMethods.Value.DuckDBCreateVarchar(collection.ToString());
        }

        using var childType = elementType.GetLogicalType();
        var values = BuildValues(childType, collection, dbType);

        return NativeMethods.Value.DuckDBCreateListValue(childType, values, collection.Count);
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

    private static DuckDBValue DecimalToDuckDBValue(decimal value)
        => NativeMethods.Value.DuckDBCreateDecimal(ToDuckDBDecimal(value));

    private static DuckDBDecimal ToDuckDBDecimal(decimal value)
    {
        var mantissa = value.GetMantissa();

        var width = mantissa.IsZero
            ? value.Scale + 1
            : Math.Max((int)BigInteger.Log10(BigInteger.Abs(mantissa)) + 1, value.Scale + 1);

        return new DuckDBDecimal((byte)width, value.Scale, new DuckDBHugeInt(mantissa));
    }
}
