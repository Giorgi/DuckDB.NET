using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Numerics;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal;

internal static class ClrToDuckDBConverter
{
    public static DuckDBValue ToDuckDBValue(this object value) =>
        value switch
        {
            bool val => NativeMethods.Value.DuckDBCreateBool(val),
            sbyte val => NativeMethods.Value.DuckDBCreateInt8(val),
            short val => NativeMethods.Value.DuckDBCreateInt16(val),
            int val => NativeMethods.Value.DuckDBCreateInt32(val),
            byte val => NativeMethods.Value.DuckDBCreateUInt8(val),
            ushort val => NativeMethods.Value.DuckDBCreateUInt16(val),
            uint val => NativeMethods.Value.DuckDBCreateUInt32(val),
            long val => NativeMethods.Value.DuckDBCreateInt64(val),
            ulong val => NativeMethods.Value.DuckDBCreateUInt64(val),
            float val => NativeMethods.Value.DuckDBCreateFloat(val),
            double val => NativeMethods.Value.DuckDBCreateDouble(val),
            string val => StringToDuckDBValue(val),
            decimal val => DecimalToDuckDBValue(val),
            Guid val => GuidToDuckDBValue(val),
            BigInteger val => NativeMethods.Value.DuckDBCreateHugeInt(new DuckDBHugeInt(val)),
            byte[] val => NativeMethods.Value.DuckDBCreateBlob(val, val.Length),
            TimeSpan val => NativeMethods.Value.DuckDBCreateInterval(val),
            DateTime val => NativeMethods.Value.DuckDBCreateTimestamp(NativeMethods.DateTimeHelpers.DuckDBToTimestamp(DuckDBTimestamp.FromDateTime(val))),
            DateTimeOffset val => DateTimeOffsetToDuckDBValue(val),
            DuckDBDateOnly val => NativeMethods.Value.DuckDBCreateDate(NativeMethods.DateTimeHelpers.DuckDBToDate(val)),
            DuckDBTimeOnly val => NativeMethods.Value.DuckDBCreateTime(NativeMethods.DateTimeHelpers.DuckDBToTime(val)),
#if NET6_0_OR_GREATER
            DateOnly val => NativeMethods.Value.DuckDBCreateDate(NativeMethods.DateTimeHelpers.DuckDBToDate(val)),
            TimeOnly val => NativeMethods.Value.DuckDBCreateTime(NativeMethods.DateTimeHelpers.DuckDBToTime(val)),
#endif
            ICollection val => CreateCollectionValue(val),
            _ => throw new InvalidCastException($"Cannot convert value of type {value.GetType().FullName} to DuckDBValue.")
        };

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

            ICollection<sbyte> items => CreateCollectionValue(DuckDBType.TinyInt, items),
            ICollection<byte> items => CreateCollectionValue(DuckDBType.UnsignedTinyInt, items),
            ICollection<short> items => CreateCollectionValue(DuckDBType.SmallInt, items),
            ICollection<ushort> items => CreateCollectionValue(DuckDBType.UnsignedSmallInt, items),
            ICollection<int> items => CreateCollectionValue(DuckDBType.Integer, items),
            ICollection<uint> items => CreateCollectionValue(DuckDBType.UnsignedInteger, items),
            ICollection<long> items => CreateCollectionValue(DuckDBType.BigInt, items),
            ICollection<ulong> items => CreateCollectionValue(DuckDBType.UnsignedBigInt, items),
            ICollection<float> items => CreateCollectionValue(DuckDBType.Float, items),
            ICollection<double> items => CreateCollectionValue(DuckDBType.Double, items),
            ICollection<BigInteger> items => CreateCollectionValue(DuckDBType.HugeInt, items),
            ICollection<decimal> items => CreateCollectionValue(DuckDBType.Varchar, items),
            ICollection<Guid> items => CreateCollectionValue(DuckDBType.Varchar, items),
            ICollection<string> items => CreateCollectionValue(DuckDBType.Varchar, items),
            ICollection<DateTime> items => CreateCollectionValue(DuckDBType.Date, items),
            ICollection<DateTimeOffset> items => CreateCollectionValue(DuckDBType.TimeTz, items),
            ICollection<TimeSpan> items => CreateCollectionValue(DuckDBType.Interval, items),
            ICollection<object> items => CreateCollectionValue(DuckDBType.List, items),
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
            if (item == null)
            {
                throw new InvalidOperationException($"Cannot convert null to DuckDBValue.");
            }
            
            var duckDBValue = item.ToDuckDBValue();
            values[index] = duckDBValue;
            index++;
        }
        
        return NativeMethods.Value.DuckDBCreateListValue(listItemType, values, collection.Count);
    }
}