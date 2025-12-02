using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Numerics;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

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
        { DbType.String, value => StringToDuckDBValue((string?)value) },
        { DbType.VarNumeric, value => NativeMethods.Value.DuckDBCreateHugeInt(new((BigInteger)value)) },
        { DbType.Binary, value =>
            {
                var bytes = (byte[])value;
                return NativeMethods.Value.DuckDBCreateBlob(bytes, bytes.Length);
            }
        },
        { DbType.Date, value =>
            {
#if NET6_0_OR_GREATER
                var date = (value is DateOnly dateOnly ? (DuckDBDateOnly)dateOnly : (DuckDBDateOnly)value).ToDuckDBDate();
#else
                var date = ((DuckDBDateOnly)value).ToDuckDBDate();
#endif
                return NativeMethods.Value.DuckDBCreateDate(date);
            }
        },
        { DbType.Time, value =>
            {
#if NET6_0_OR_GREATER
                var time = NativeMethods.DateTimeHelpers.DuckDBToTime(value is TimeOnly timeOnly ? (DuckDBTimeOnly)timeOnly : (DuckDBTimeOnly)value);
#else
                var time = NativeMethods.DateTimeHelpers.DuckDBToTime((DuckDBTimeOnly)value);
#endif
                return NativeMethods.Value.DuckDBCreateTime(time);
            }
        },
        { DbType.DateTime, value => NativeMethods.Value.DuckDBCreateTimestamp(((DateTime)value).ToTimestampStruct(DuckDBType.Timestamp))},
        { DbType.DateTimeOffset, value => NativeMethods.Value.DuckDBCreateTimestampTz(((DateTimeOffset)value).ToTimestampStruct()) },
    };

    public static DuckDBValue ToDuckDBValue(this object? item, DuckDBLogicalType logicalType, DuckDBType duckDBType, DbType dbType)
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
#if NET6_0_OR_GREATER
            (DuckDBType.Date, DateOnly value) => NativeMethods.Value.DuckDBCreateDate(((DuckDBDateOnly)value).ToDuckDBDate()),
            (DuckDBType.Time, TimeOnly value) => NativeMethods.Value.DuckDBCreateTime(NativeMethods.DateTimeHelpers.DuckDBToTime(value)),
#endif
            (DuckDBType.TimeTz, DateTimeOffset value) => NativeMethods.Value.DuckDBCreateTimeTz(value.ToTimeTzStruct()),
            (DuckDBType.Blob, byte[] value) => NativeMethods.Value.DuckDBCreateBlob(value, value.Length),
            (DuckDBType.List, ICollection value) => CreateCollectionValue(logicalType, value, true, dbType),
            (DuckDBType.Array, ICollection value) => CreateCollectionValue(logicalType, value, false, dbType),
            _ when ValueCreators.TryGetValue(dbType, out var converter) => converter(item),
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

    private static DuckDBValue CreateCollectionValue(DuckDBLogicalType logicalType, ICollection collection, bool isList, DbType dbType)
    {
        using var collectionItemType = isList ? NativeMethods.LogicalType.DuckDBListTypeChildType(logicalType) :
                                                NativeMethods.LogicalType.DuckDBArrayTypeChildType(logicalType);

        var duckDBType = NativeMethods.LogicalType.DuckDBGetTypeId(collectionItemType);

        var values = new DuckDBValue[collection.Count];

        var index = 0;
        foreach (var item in collection)
        {
            var duckDBValue = item.ToDuckDBValue(collectionItemType, duckDBType, dbType);
            values[index] = duckDBValue;
            index++;
        }

        return isList ? NativeMethods.Value.DuckDBCreateListValue(collectionItemType, values, collection.Count)
                      : NativeMethods.Value.DuckDBCreateArrayValue(collectionItemType, values, collection.Count);
    }

    private static DuckDBValue StringToDuckDBValue(string? value)
    {
        using var handle = value.ToUnmanagedString();
        return NativeMethods.Value.DuckDBCreateVarchar(handle);
    }

    private static DuckDBValue DecimalToDuckDBValue(decimal value)
    {
        var bits = decimal.GetBits(value);
        var scale = (byte)((bits[3] >> 16) & 0x7F);

        var power = Math.Pow(10, scale);

        var integralPart = decimal.Truncate(value);
        var fractionalPart = value - integralPart;

        var result = BigInteger.Multiply(new BigInteger(integralPart), new BigInteger(power));

        result += new BigInteger(decimal.Multiply(fractionalPart, (decimal)power));

        var width = integralPart == 0 ? scale + 1 : (int)Math.Floor(BigInteger.Log10(BigInteger.Abs(result))) + 1;

        return NativeMethods.Value.DuckDBCreateDecimal(new DuckDBDecimal((byte)width, scale, new DuckDBHugeInt(result)));
    }
}