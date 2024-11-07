using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Numerics;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal;

internal static class ClrToDuckDBConverter
{
    public static DuckDBValue ToDuckDBValue(this object? value)
    {
        if (value.IsNull())
        {
            return NativeMethods.Value.DuckDBCreateNullValue();
        }

        return value switch
        {
            bool val => NativeMethods.Value.DuckDBCreateBool(val),

            sbyte val => NativeMethods.Value.DuckDBCreateInt8(val),
            short val => NativeMethods.Value.DuckDBCreateInt16(val),
            int val => NativeMethods.Value.DuckDBCreateInt32(val),
            long val => NativeMethods.Value.DuckDBCreateInt64(val),

            byte val => NativeMethods.Value.DuckDBCreateUInt8(val),
            ushort val => NativeMethods.Value.DuckDBCreateUInt16(val),
            uint val => NativeMethods.Value.DuckDBCreateUInt32(val),
            ulong val => NativeMethods.Value.DuckDBCreateUInt64(val),

            float val => NativeMethods.Value.DuckDBCreateFloat(val),
            double val => NativeMethods.Value.DuckDBCreateDouble(val),

            decimal val => DecimalToDuckDBValue(val),
            BigInteger val => NativeMethods.Value.DuckDBCreateHugeInt(new DuckDBHugeInt(val)),

            string val => StringToDuckDBValue(val),
            Guid val => GuidToDuckDBValue(val),
            DateTime val => NativeMethods.Value.DuckDBCreateTimestamp(NativeMethods.DateTimeHelpers.DuckDBToTimestamp(DuckDBTimestamp.FromDateTime(val))),
            TimeSpan val => NativeMethods.Value.DuckDBCreateInterval(val),
            DuckDBDateOnly val => NativeMethods.Value.DuckDBCreateDate(NativeMethods.DateTimeHelpers.DuckDBToDate(val)),
            DuckDBTimeOnly val => NativeMethods.Value.DuckDBCreateTime(NativeMethods.DateTimeHelpers.DuckDBToTime(val)),
#if NET6_0_OR_GREATER
            DateOnly val => NativeMethods.Value.DuckDBCreateDate(NativeMethods.DateTimeHelpers.DuckDBToDate(val)),
            TimeOnly val => NativeMethods.Value.DuckDBCreateTime(NativeMethods.DateTimeHelpers.DuckDBToTime(val)),
#endif
            DateTimeOffset val => DateTimeOffsetToDuckDBValue(val),
            byte[] val => NativeMethods.Value.DuckDBCreateBlob(val, val.Length),

            ICollection val => CreateCollectionValue(val),
            _ => throw new InvalidCastException($"Cannot convert value of type {value.GetType().FullName} to DuckDBValue.")
        };
    }

    private static DuckDBValue DateTimeOffsetToDuckDBValue(DateTimeOffset val)
    {
        var duckDBToTime = NativeMethods.DateTimeHelpers.DuckDBToTime((DuckDBTimeOnly)val.DateTime);
        var duckDBCreateTimeTz = NativeMethods.DateTimeHelpers.DuckDBCreateTimeTz(duckDBToTime.Micros, (int)val.Offset.TotalSeconds);
        return NativeMethods.Value.DuckDBCreateTimeTz(duckDBCreateTimeTz);
    }

    private static DuckDBValue GuidToDuckDBValue(Guid value)
    {
        using var handle = value.ToString().ToUnmanagedString();
        return NativeMethods.Value.DuckDBCreateVarchar(handle);
    }

    private static DuckDBValue DecimalToDuckDBValue(decimal value)
    {
        using var handle = value.ToString(CultureInfo.InvariantCulture).ToUnmanagedString();
        return NativeMethods.Value.DuckDBCreateVarchar(handle);
    }

    private static DuckDBValue StringToDuckDBValue(string value)
    {
        using var handle = value.ToUnmanagedString();
        return NativeMethods.Value.DuckDBCreateVarchar(handle);
    }

    private static DuckDBValue CreateCollectionValue(ICollection collection)
    {
        return collection switch
        {
            ICollection<bool> items => CreateCollectionValue(DuckDBType.Boolean, items),
            ICollection<bool?> items => CreateCollectionValue(DuckDBType.Boolean, items),

            ICollection<sbyte> items => CreateCollectionValue(DuckDBType.TinyInt, items),
            ICollection<sbyte?> items => CreateCollectionValue(DuckDBType.TinyInt, items),
            ICollection<short> items => CreateCollectionValue(DuckDBType.SmallInt, items),
            ICollection<short?> items => CreateCollectionValue(DuckDBType.SmallInt, items),
            ICollection<int> items => CreateCollectionValue(DuckDBType.Integer, items),
            ICollection<int?> items => CreateCollectionValue(DuckDBType.Integer, items),
            ICollection<long> items => CreateCollectionValue(DuckDBType.BigInt, items),
            ICollection<long?> items => CreateCollectionValue(DuckDBType.BigInt, items),

            ICollection<byte> items => CreateCollectionValue(DuckDBType.UnsignedTinyInt, items),
            ICollection<byte?> items => CreateCollectionValue(DuckDBType.UnsignedTinyInt, items),
            ICollection<ushort> items => CreateCollectionValue(DuckDBType.UnsignedSmallInt, items),
            ICollection<ushort?> items => CreateCollectionValue(DuckDBType.UnsignedSmallInt, items),
            ICollection<uint> items => CreateCollectionValue(DuckDBType.UnsignedInteger, items),
            ICollection<uint?> items => CreateCollectionValue(DuckDBType.UnsignedInteger, items),
            ICollection<ulong> items => CreateCollectionValue(DuckDBType.UnsignedBigInt, items),
            ICollection<ulong?> items => CreateCollectionValue(DuckDBType.UnsignedBigInt, items),

            ICollection<float> items => CreateCollectionValue(DuckDBType.Float, items),
            ICollection<float?> items => CreateCollectionValue(DuckDBType.Float, items),
            ICollection<double> items => CreateCollectionValue(DuckDBType.Double, items),
            ICollection<double?> items => CreateCollectionValue(DuckDBType.Double, items),

            ICollection<decimal> items => CreateCollectionValue(DuckDBType.Varchar, items),
            ICollection<decimal?> items => CreateCollectionValue(DuckDBType.Varchar, items),
            ICollection<BigInteger> items => CreateCollectionValue(DuckDBType.HugeInt, items),
            ICollection<BigInteger?> items => CreateCollectionValue(DuckDBType.HugeInt, items),

            ICollection<string> items => CreateCollectionValue(DuckDBType.Varchar, items),
            ICollection<Guid> items => CreateCollectionValue(DuckDBType.Varchar, items),
            ICollection<Guid?> items => CreateCollectionValue(DuckDBType.Varchar, items),
            ICollection<DateTime> items => CreateCollectionValue(DuckDBType.Date, items),
            ICollection<DateTime?> items => CreateCollectionValue(DuckDBType.Date, items),
            ICollection<TimeSpan> items => CreateCollectionValue(DuckDBType.Interval, items),
            ICollection<TimeSpan?> items => CreateCollectionValue(DuckDBType.Interval, items),
            ICollection<DuckDBDateOnly> items => CreateCollectionValue(DuckDBType.Date, items),
            ICollection<DuckDBDateOnly?> items => CreateCollectionValue(DuckDBType.Date, items),
            ICollection<DuckDBTimeOnly> items => CreateCollectionValue(DuckDBType.Time, items),
            ICollection<DuckDBTimeOnly?> items => CreateCollectionValue(DuckDBType.Time, items),
#if NET6_0_OR_GREATER
            ICollection<DateOnly> items => CreateCollectionValue(DuckDBType.Date, items),
            ICollection<DateOnly?> items => CreateCollectionValue(DuckDBType.Date, items),
            ICollection<TimeOnly> items => CreateCollectionValue(DuckDBType.Time, items),
            ICollection<TimeOnly?> items => CreateCollectionValue(DuckDBType.Time, items),
#endif
            ICollection<DateTimeOffset> items => CreateCollectionValue(DuckDBType.TimeTz, items),
            ICollection<DateTimeOffset?> items => CreateCollectionValue(DuckDBType.TimeTz, items),
            _ => throw new InvalidOperationException($"Cannot convert collection type {collection.GetType().FullName} to DuckDBValue.")
        };
    }

    private static DuckDBValue CreateCollectionValue<T>(DuckDBType duckDBType, ICollection<T> collection)
    {
        using var listItemType = NativeMethods.LogicalType.DuckDBCreateLogicalType(duckDBType);

        var values = new DuckDBValue[collection.Count];

        var index = 0;
        foreach (var item in collection)
        {
            var duckDBValue = item.ToDuckDBValue();
            values[index] = duckDBValue;
            index++;
        }

        return NativeMethods.Value.DuckDBCreateListValue(listItemType, values, collection.Count);
    }
}