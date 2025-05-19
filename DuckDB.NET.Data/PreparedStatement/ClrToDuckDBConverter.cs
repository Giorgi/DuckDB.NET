using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;
using System;
using System.Collections;
using System.Globalization;
using System.Numerics;

namespace DuckDB.NET.Data.PreparedStatement;

internal static class ClrToDuckDBConverter
{
    public static DuckDBValue ToDuckDBValue(this object? item, DuckDBLogicalType logicalType, DuckDBType duckDBType)
    {
        if (item.IsNull() || item == null) //item == null is redundant but net standard can't understand that item isn't null after this point.
        {
            return NativeMethods.Value.DuckDBCreateNullValue();
        }

        return (duckDBType, item) switch
        {
            (DuckDBType.Boolean, bool value) => NativeMethods.Value.DuckDBCreateBool(value),

            (DuckDBType.TinyInt, _) => TryConvertTo<sbyte>(out var result) ? NativeMethods.Value.DuckDBCreateInt8(result) : StringToDuckDBValue(item.ToString()),
            (DuckDBType.SmallInt, _) => TryConvertTo<short>(out var result) ? NativeMethods.Value.DuckDBCreateInt16(result) : StringToDuckDBValue(item.ToString()),
            (DuckDBType.Integer, _) => TryConvertTo<int>(out var result) ? NativeMethods.Value.DuckDBCreateInt32(result) : StringToDuckDBValue(item.ToString()),
            (DuckDBType.BigInt, _) => TryConvertTo<long>(out var result) ? NativeMethods.Value.DuckDBCreateInt64(result) : StringToDuckDBValue(item.ToString()),

            (DuckDBType.UnsignedTinyInt, _) => TryConvertTo<byte>(out var result) ? NativeMethods.Value.DuckDBCreateUInt8(result) : StringToDuckDBValue(item.ToString()),
            (DuckDBType.UnsignedSmallInt, _) => TryConvertTo<ushort>(out var result) ? NativeMethods.Value.DuckDBCreateUInt16(result) : StringToDuckDBValue(item.ToString()),
            (DuckDBType.UnsignedInteger, _) => TryConvertTo<uint>(out var result) ? NativeMethods.Value.DuckDBCreateUInt32(result) : StringToDuckDBValue(item.ToString()),
            (DuckDBType.UnsignedBigInt, _) => TryConvertTo<ulong>(out var result) ? NativeMethods.Value.DuckDBCreateUInt64(result) : StringToDuckDBValue(item.ToString()),

            (DuckDBType.Float, float value) => NativeMethods.Value.DuckDBCreateFloat(value),
            (DuckDBType.Double, double value) => NativeMethods.Value.DuckDBCreateDouble(value),

            (DuckDBType.Decimal, decimal value) => DecimalToDuckDBValue(value),
            (DuckDBType.HugeInt, BigInteger value) => NativeMethods.Value.DuckDBCreateHugeInt(new DuckDBHugeInt(value)),

            (DuckDBType.Varchar, string value) => StringToDuckDBValue(value),
            (DuckDBType.Uuid, Guid value) => GuidToDuckDBValue(value),

            (DuckDBType.Timestamp, DateTime value) => NativeMethods.Value.DuckDBCreateTimestamp(value.ToTimestampStruct(duckDBType)),
            (DuckDBType.TimestampS, DateTime value) => NativeMethods.Value.DuckDBCreateTimestampS(value.ToTimestampStruct(duckDBType)),
            (DuckDBType.TimestampMs, DateTime value) => NativeMethods.Value.DuckDBCreateTimestampMs(value.ToTimestampStruct(duckDBType)),
            (DuckDBType.TimestampNs, DateTime value) => NativeMethods.Value.DuckDBCreateTimestampNs(value.ToTimestampStruct(duckDBType)),
            (DuckDBType.TimestampTz, DateTime value) => NativeMethods.Value.DuckDBCreateTimestampTz(value.ToTimestampStruct(duckDBType)),
            (DuckDBType.TimestampTz, DateTimeOffset value) => NativeMethods.Value.DuckDBCreateTimestampTz(value.ToTimestampStruct()),
            (DuckDBType.Interval, TimeSpan value) => NativeMethods.Value.DuckDBCreateInterval(value),
            (_, DateTime value) => StringToDuckDBValue(value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)),
            (_, DateTimeOffset value) => StringToDuckDBValue(value.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture)),
            (DuckDBType.Date, DateTime value) => NativeMethods.Value.DuckDBCreateDate(NativeMethods.DateTimeHelpers.DuckDBToDate((DuckDBDateOnly)value)),
            (DuckDBType.Date, DuckDBDateOnly value) => NativeMethods.Value.DuckDBCreateDate(NativeMethods.DateTimeHelpers.DuckDBToDate(value)),
            (DuckDBType.Time, DateTime value) => NativeMethods.Value.DuckDBCreateTime(NativeMethods.DateTimeHelpers.DuckDBToTime((DuckDBTimeOnly)value)),
            (DuckDBType.Time, DuckDBTimeOnly value) => NativeMethods.Value.DuckDBCreateTime(NativeMethods.DateTimeHelpers.DuckDBToTime(value)),
#if NET6_0_OR_GREATER
            (DuckDBType.Date, DateOnly value) => NativeMethods.Value.DuckDBCreateDate(NativeMethods.DateTimeHelpers.DuckDBToDate(value)),
            (DuckDBType.Time, TimeOnly value) => NativeMethods.Value.DuckDBCreateTime(NativeMethods.DateTimeHelpers.DuckDBToTime(value)),
#endif
            (DuckDBType.TimeTz, DateTimeOffset value) => NativeMethods.Value.DuckDBCreateTimeTz(value.ToTimeTzStruct()),
            (DuckDBType.Blob, byte[] value) => NativeMethods.Value.DuckDBCreateBlob(value, value.Length),
            (DuckDBType.List, ICollection value) => CreateCollectionValue(logicalType, value, true),
            (DuckDBType.Array, ICollection value) => CreateCollectionValue(logicalType, value, false),
            _ => StringToDuckDBValue(item.ToString())
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

    private static DuckDBValue StringToDuckDBValue(string? value)
    {
        using var handle = value.ToUnmanagedString();
        return NativeMethods.Value.DuckDBCreateVarchar(handle);
    }

    private static DuckDBValue DecimalToDuckDBValue(decimal value)
    {
        using var handle = value.ToString(CultureInfo.InvariantCulture).ToUnmanagedString();
        return NativeMethods.Value.DuckDBCreateVarchar(handle);
    }
}