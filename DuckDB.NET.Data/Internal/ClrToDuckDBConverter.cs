using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;
using System;
using System.Collections;
using System.Globalization;
using System.Numerics;

namespace DuckDB.NET.Data.Internal;

internal static class ClrToDuckDBConverter
{
    public static DuckDBValue ToDuckDBValue(this object? item, DuckDBLogicalType logicalType, DuckDBType duckDBType)
    {
        if (item.IsNull())
        {
            return NativeMethods.Value.DuckDBCreateNullValue();
        }

        return (duckDBType, item) switch
        {
            (DuckDBType.Boolean, bool value) => NativeMethods.Value.DuckDBCreateBool(value),

            (DuckDBType.TinyInt, _) => NativeMethods.Value.DuckDBCreateInt8(ConvertTo<sbyte>()),
            (DuckDBType.SmallInt, _) => NativeMethods.Value.DuckDBCreateInt16(ConvertTo<short>()),
            (DuckDBType.Integer, _) => NativeMethods.Value.DuckDBCreateInt32(ConvertTo<int>()),
            (DuckDBType.BigInt, _) => NativeMethods.Value.DuckDBCreateInt64(ConvertTo<long>()),

            (DuckDBType.UnsignedTinyInt, _) => NativeMethods.Value.DuckDBCreateUInt8(ConvertTo<byte>()),
            (DuckDBType.UnsignedSmallInt, _) => NativeMethods.Value.DuckDBCreateUInt16(ConvertTo<ushort>()),
            (DuckDBType.UnsignedInteger, _) => NativeMethods.Value.DuckDBCreateUInt32(ConvertTo<uint>()),
            (DuckDBType.UnsignedBigInt, _) => NativeMethods.Value.DuckDBCreateUInt64(ConvertTo<ulong>()),

            (DuckDBType.Float, float value) => NativeMethods.Value.DuckDBCreateFloat(value),
            (DuckDBType.Double, double value) => NativeMethods.Value.DuckDBCreateDouble(value),

            (DuckDBType.Decimal, decimal value) => DecimalToDuckDBValue(value),
            (DuckDBType.HugeInt, BigInteger value) => NativeMethods.Value.DuckDBCreateHugeInt(new DuckDBHugeInt(value)),

            (DuckDBType.Varchar, string value) => StringToDuckDBValue(value),
            (DuckDBType.Uuid, Guid value) => GuidToDuckDBValue(value),

            (DuckDBType.Timestamp, DateTime value) => DateTimeToTimestamp(DuckDBType.Timestamp, value),
            (DuckDBType.TimestampS, DateTime value) => DateTimeToTimestamp(DuckDBType.TimestampS, value, divisor: 1000000),
            (DuckDBType.TimestampMs, DateTime value) => DateTimeToTimestamp(DuckDBType.TimestampMs, value, divisor: 1000),
            (DuckDBType.TimestampNs, DateTime value) => DateTimeToTimestamp(DuckDBType.TimestampNs, value, factor: 1000, extra: value.Nanoseconds()),
            (DuckDBType.Interval, TimeSpan value) => NativeMethods.Value.DuckDBCreateInterval(value),
            (DuckDBType.Date, DateTime value) => NativeMethods.Value.DuckDBCreateDate(NativeMethods.DateTimeHelpers.DuckDBToDate((DuckDBDateOnly)value)),
            (DuckDBType.Date, DuckDBDateOnly value) => NativeMethods.Value.DuckDBCreateDate(NativeMethods.DateTimeHelpers.DuckDBToDate(value)),
            (DuckDBType.Time, DateTime value) => NativeMethods.Value.DuckDBCreateTime(NativeMethods.DateTimeHelpers.DuckDBToTime((DuckDBTimeOnly)value)),
            (DuckDBType.Time, DuckDBTimeOnly value) => NativeMethods.Value.DuckDBCreateTime(NativeMethods.DateTimeHelpers.DuckDBToTime(value)),
#if NET6_0_OR_GREATER
            (DuckDBType.Date, DateOnly value) => NativeMethods.Value.DuckDBCreateDate(NativeMethods.DateTimeHelpers.DuckDBToDate(value)),
            (DuckDBType.Time, TimeOnly value) => NativeMethods.Value.DuckDBCreateTime(NativeMethods.DateTimeHelpers.DuckDBToTime(value)),
#endif
            (DuckDBType.TimeTz, DateTimeOffset value) => DateTimeOffsetToTimeTzDuckDBValue(value),
            (DuckDBType.TimestampTz, DateTimeOffset value) => NativeMethods.Value.DuckDBCreateTimestamp(NativeMethods.DateTimeHelpers.DuckDBToTimestamp(DuckDBTimestamp.FromDateTime(value.DateTime))),
            (DuckDBType.Blob, byte[] value) => NativeMethods.Value.DuckDBCreateBlob(value, value.Length),
            (DuckDBType.List, ICollection value) => CreateCollectionValue(logicalType, value, true),
            (DuckDBType.Array, ICollection value) => CreateCollectionValue(logicalType, value, false),
            _ => throw new InvalidOperationException($"Cannot bind parameter type {item.GetType().FullName} to column of type {duckDBType}")
        };

        T ConvertTo<T>()
        {
            try
            {
                return (T)Convert.ChangeType(item, typeof(T));
            }
            catch (Exception)
            {
                throw new ArgumentOutOfRangeException($"Cannot bind parameter type {item.GetType().FullName} to column of type {duckDBType}");
            }
        }

        DuckDBValue DateTimeToTimestamp(DuckDBType type, DateTime value, int factor = 1, int divisor = 1, int extra = 0)
        {
            var timestamp = NativeMethods.DateTimeHelpers.DuckDBToTimestamp(DuckDBTimestamp.FromDateTime(value));

            timestamp.Micros = timestamp.Micros * factor / divisor;

            return type switch
            {
                DuckDBType.Timestamp => NativeMethods.Value.DuckDBCreateTimestamp(timestamp),
                DuckDBType.TimestampS => NativeMethods.Value.DuckDBCreateTimestampS(timestamp),
                DuckDBType.TimestampMs => NativeMethods.Value.DuckDBCreateTimestampMs(timestamp),
                DuckDBType.TimestampNs => NativeMethods.Value.DuckDBCreateTimestampNs(timestamp),
            };
        }
    }

    private static DuckDBValue CreateCollectionValue(DuckDBLogicalType logicalType, ICollection collection, bool isList)
    {
        using var collectionItemType = isList ? NativeMethods.LogicalType.DuckDBListTypeChildType(logicalType) :
                                                NativeMethods.LogicalType.DuckDBArrayTypeChildType(logicalType);

        var duckDBType = NativeMethods.LogicalType.DuckDBGetTypeId(collectionItemType);

        var values = new DuckDBValue[collection.Count];

        var index = 0;
        foreach (var item in collection)
        {
            var duckDBValue = item.ToDuckDBValue(collectionItemType, duckDBType);
            values[index] = duckDBValue;
            index++;
        }

        return isList ? NativeMethods.Value.DuckDBCreateListValue(collectionItemType, values, collection.Count)
                      : NativeMethods.Value.DuckDBCreateArrayValue(collectionItemType, values, collection.Count);
    }

    private static DuckDBValue GuidToDuckDBValue(Guid value)
    {
        using var handle = value.ToString().ToUnmanagedString();
        return NativeMethods.Value.DuckDBCreateVarchar(handle);
    }

    private static DuckDBValue StringToDuckDBValue(string value)
    {
        using var handle = value.ToUnmanagedString();
        return NativeMethods.Value.DuckDBCreateVarchar(handle);
    }

    private static DuckDBValue DecimalToDuckDBValue(decimal value)
    {
        using var handle = value.ToString(CultureInfo.InvariantCulture).ToUnmanagedString();
        return NativeMethods.Value.DuckDBCreateVarchar(handle);
    }

    private static DuckDBValue DateTimeOffsetToTimeTzDuckDBValue(DateTimeOffset val)
    {
        var duckDBToTime = NativeMethods.DateTimeHelpers.DuckDBToTime((DuckDBTimeOnly)val.DateTime);
        var duckDBCreateTimeTz = NativeMethods.DateTimeHelpers.DuckDBCreateTimeTz(duckDBToTime.Micros, (int)val.Offset.TotalSeconds);
        return NativeMethods.Value.DuckDBCreateTimeTz(duckDBCreateTimeTz);
    }
}