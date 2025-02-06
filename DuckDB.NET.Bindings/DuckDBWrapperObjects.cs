using Microsoft.Win32.SafeHandles;
using System;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Native;

public class DuckDBDatabase() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        NativeMethods.Startup.DuckDBClose(out handle);
        return true;
    }
}

public class DuckDBNativeConnection() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        NativeMethods.Startup.DuckDBDisconnect(out handle);
        return true;
    }

    public void Interrupt()
    {
        NativeMethods.Startup.DuckDBInterrupt(this);
    }
}

public class DuckDBPreparedStatement() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        NativeMethods.PreparedStatements.DuckDBDestroyPrepare(out handle);
        return true;
    }
}

public class DuckDBConfig() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        NativeMethods.Configuration.DuckDBDestroyConfig(out handle);
        return true;
    }
}

public class DuckDBAppender() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        return NativeMethods.Appender.DuckDBDestroyAppender(out handle).IsSuccess();
    }
}

public class DuckDBExtractedStatements() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        NativeMethods.ExtractStatements.DuckDBDestroyExtracted(out handle);

        return true;
    }
}

public class DuckDBLogicalType() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        NativeMethods.LogicalType.DuckDBDestroyLogicalType(out handle);
        return true;
    }
}

public class DuckDBDataChunk : SafeHandleZeroOrMinusOneIsInvalid
{
    public DuckDBDataChunk() : base(true)
    {
    }

    public DuckDBDataChunk(IntPtr chunk) : base(false)
    {
        SetHandle(chunk);
    }

    protected override bool ReleaseHandle()
    {
        NativeMethods.DataChunks.DuckDBDestroyDataChunk(out handle);
        return true;
    }
}

public class DuckDBValue() : SafeHandleZeroOrMinusOneIsInvalid(true), IDuckDBValueReader
{
    private DuckDBValue[] childValues = [];

    protected override bool ReleaseHandle()
    {
        foreach (var value in childValues)
        {
            value.Dispose();
        }
        
        NativeMethods.Value.DuckDBDestroyValue(out handle);
        return true;
    }

    internal void SetChildValues(DuckDBValue[] values)
    {
        childValues = values;
    }

    public bool IsNull() => NativeMethods.Value.DuckDBIsNullValue(this);

    public T GetValue<T>()
    {
        var logicalType = NativeMethods.Value.DuckDBGetValueType(this);

        //Logical type is part of the duckdb_value object and it shouldn't be released separately
        //It will get released when the duckdb_value object is destroyed below.
        var add = false;
        logicalType.DangerousAddRef(ref add);

        var duckDBType = NativeMethods.LogicalType.DuckDBGetTypeId(logicalType);

        return duckDBType switch
        {
            DuckDBType.Boolean => Cast(NativeMethods.Value.DuckDBGetBool(this)),

            DuckDBType.TinyInt => Cast(NativeMethods.Value.DuckDBGetInt8(this)),
            DuckDBType.SmallInt => Cast(NativeMethods.Value.DuckDBGetInt16(this)),
            DuckDBType.Integer => Cast(NativeMethods.Value.DuckDBGetInt32(this)),
            DuckDBType.BigInt => Cast(NativeMethods.Value.DuckDBGetInt64(this)),

            DuckDBType.UnsignedTinyInt => Cast(NativeMethods.Value.DuckDBGetUInt8(this)),
            DuckDBType.UnsignedSmallInt => Cast(NativeMethods.Value.DuckDBGetUInt16(this)),
            DuckDBType.UnsignedInteger => Cast(NativeMethods.Value.DuckDBGetUInt32(this)),
            DuckDBType.UnsignedBigInt => Cast(NativeMethods.Value.DuckDBGetUInt64(this)),

            DuckDBType.Float => Cast(NativeMethods.Value.DuckDBGetFloat(this)),
            DuckDBType.Double => Cast(NativeMethods.Value.DuckDBGetDouble(this)),
            
            DuckDBType.Decimal => Cast(decimal.Parse(NativeMethods.Value.DuckDBGetVarchar(this))),
            DuckDBType.Uuid => Cast(new Guid(NativeMethods.Value.DuckDBGetVarchar(this))),
            
            DuckDBType.HugeInt => Cast(NativeMethods.Value.DuckDBGetHugeInt(this).ToBigInteger()),
            DuckDBType.UnsignedHugeInt => Cast(NativeMethods.Value.DuckDBGetUHugeInt(this).ToBigInteger()),
            
            DuckDBType.Varchar => Cast(NativeMethods.Value.DuckDBGetVarchar(this)),

#if NET6_0_OR_GREATER
            DuckDBType.Date => Cast((DateOnly)NativeMethods.DateTimeHelpers.DuckDBFromDate(NativeMethods.Value.DuckDBGetDate(this))),
            DuckDBType.Time => Cast((TimeOnly)NativeMethods.DateTimeHelpers.DuckDBFromTime(NativeMethods.Value.DuckDBGetTime(this))),
#else
            DuckDBType.Date => Cast(NativeMethods.DateTimeHelpers.DuckDBFromDate(NativeMethods.Value.DuckDBGetDate(this)).ToDateTime()),
            DuckDBType.Time => Cast(NativeMethods.DateTimeHelpers.DuckDBFromTime(NativeMethods.Value.DuckDBGetTime(this)).ToDateTime()),
#endif
            //DuckDBType.TimeTz => expr,
            DuckDBType.Interval => Cast((TimeSpan)NativeMethods.Value.DuckDBGetInterval(this)),
            DuckDBType.Timestamp => Cast(NativeMethods.DateTimeHelpers.DuckDBFromTimestamp(NativeMethods.Value.DuckDBGetTimestamp(this)).ToDateTime()),
            //DuckDBType.TimestampS => expr,
            //DuckDBType.TimestampMs => expr,
            //DuckDBType.TimestampNs => expr,
            //DuckDBType.TimestampTz => expr,
            _ => throw new NotImplementedException($"Cannot read value of type {typeof(T).FullName}")
        };

        T Cast<TSource>(TSource value) => Unsafe.As<TSource, T>(ref value);
    }
}
