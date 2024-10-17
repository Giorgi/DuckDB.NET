using System;
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
            DateTime val => NativeMethods.Value.DuckDBCreateTimestamp(NativeMethods.DateTimeHelpers.DuckDBToTimestamp(DuckDBTimestamp.FromDateTime(val))),
            DuckDBDateOnly val => NativeMethods.Value.DuckDBCreateDate(NativeMethods.DateTimeHelpers.DuckDBToDate(val)),
#if NET6_0_OR_GREATER
            DateOnly val => NativeMethods.Value.DuckDBCreateDate(NativeMethods.DateTimeHelpers.DuckDBToDate(val)),
#endif
            DuckDBTimeOnly val => NativeMethods.Value.DuckDBCreateTime(NativeMethods.DateTimeHelpers.DuckDBToTime(val)),
#if NET6_0_OR_GREATER
            TimeOnly val => NativeMethods.Value.DuckDBCreateTime(NativeMethods.DateTimeHelpers.DuckDBToTime(val)),
#endif
            ICollection<int> val => CreateListValue(DuckDBType.Integer, val),
            _ => throw new InvalidCastException($"Cannot convert value of type {value.GetType().FullName} to DuckDBValue.")
        };

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
    
    private static DuckDBValue CreateListValue<T>(DuckDBType duckDBType, ICollection<T> collection)
    {
        using var logicalType = NativeMethods.LogicalType.DuckDBCreateLogicalType(duckDBType);
        
        var values = new DuckDBValue[collection.Count];
        
        var index = 0;
        foreach (var item in collection)
        {
            var duckDBValue = item.ToDuckDBValue();
            values[index] = duckDBValue;
            index++;
        }
        
        return NativeMethods.Value.DuckDBCreateListValue(logicalType, values, collection.Count);
    }
}