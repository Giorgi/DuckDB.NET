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

            (DuckDBType.TinyInt, _) => TryConvertTo<sbyte>(out var result) ? NativeMethods.Value.DuckDBCreateInt8(result) : NativeMethods.Value.DuckDBCreateVarchar(item.ToString()),
            (DuckDBType.SmallInt, _) => TryConvertTo<short>(out var result) ? NativeMethods.Value.DuckDBCreateInt16(result) : NativeMethods.Value.DuckDBCreateVarchar(item.ToString()),
            (DuckDBType.Integer, _) => TryConvertTo<int>(out var result) ? NativeMethods.Value.DuckDBCreateInt32(result) : NativeMethods.Value.DuckDBCreateVarchar(item.ToString()),
            (DuckDBType.BigInt, _) => TryConvertTo<long>(out var result) ? NativeMethods.Value.DuckDBCreateInt64(result) : NativeMethods.Value.DuckDBCreateVarchar(item.ToString()),

            (DuckDBType.UnsignedTinyInt, _) => TryConvertTo<byte>(out var result) ? NativeMethods.Value.DuckDBCreateUInt8(result) : NativeMethods.Value.DuckDBCreateVarchar(item.ToString()),
            (DuckDBType.UnsignedSmallInt, _) => TryConvertTo<ushort>(out var result) ? NativeMethods.Value.DuckDBCreateUInt16(result) : NativeMethods.Value.DuckDBCreateVarchar(item.ToString()),
            (DuckDBType.UnsignedInteger, _) => TryConvertTo<uint>(out var result) ? NativeMethods.Value.DuckDBCreateUInt32(result) : NativeMethods.Value.DuckDBCreateVarchar(item.ToString()),
            (DuckDBType.UnsignedBigInt, _) => TryConvertTo<ulong>(out var result) ? NativeMethods.Value.DuckDBCreateUInt64(result) : NativeMethods.Value.DuckDBCreateVarchar(item.ToString()),

            (DuckDBType.Float, float value) => NativeMethods.Value.DuckDBCreateFloat(value),
            (DuckDBType.Double, double value) => NativeMethods.Value.DuckDBCreateDouble(value),

            (DuckDBType.Decimal, decimal value) => DecimalToDuckDBValue(value),
            (DuckDBType.HugeInt, BigInteger value) => NativeMethods.Value.DuckDBCreateHugeInt(new DuckDBHugeInt(value)),
            (DuckDBType.VarInt, BigInteger value) => BigIntegerToDuckDBValue(value),

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

        bool TryConvertTo<T>(out T result) where T : struct
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

    private static DuckDBValue DecimalToDuckDBValue(decimal value)
    {
        var mantissa = value.GetMantissa();

        var width = mantissa.IsZero
            ? value.Scale + 1
            : Math.Max((int)BigInteger.Log10(BigInteger.Abs(mantissa)) + 1, value.Scale + 1);

        return NativeMethods.Value.DuckDBCreateDecimal(new DuckDBDecimal((byte)width, value.Scale, new DuckDBHugeInt(mantissa)));
    }
}
