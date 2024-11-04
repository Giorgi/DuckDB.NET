using Microsoft.Win32.SafeHandles;
using System;
using System.Numerics;
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
        return NativeMethods.Appender.DuckDBDestroyAppender(out handle) == DuckDBState.Success;
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
            DuckDBType.Boolean => ReadValue<bool>(NativeMethods.Value.DuckDBGetBool(this)),

            DuckDBType.TinyInt => ReadValue<sbyte>(NativeMethods.Value.DuckDBGetInt8(this)),
            DuckDBType.SmallInt => ReadValue<short>(NativeMethods.Value.DuckDBGetInt16(this)),
            DuckDBType.Integer => ReadValue<int>(NativeMethods.Value.DuckDBGetInt32(this)),
            DuckDBType.BigInt => ReadValue<long>(NativeMethods.Value.DuckDBGetInt64(this)),

            DuckDBType.UnsignedTinyInt => ReadValue<byte>(NativeMethods.Value.DuckDBGetUInt8(this)),
            DuckDBType.UnsignedSmallInt => ReadValue<ushort>(NativeMethods.Value.DuckDBGetUInt16(this)),
            DuckDBType.UnsignedInteger => ReadValue<uint>(NativeMethods.Value.DuckDBGetUInt32(this)),
            DuckDBType.UnsignedBigInt => ReadValue<ulong>(NativeMethods.Value.DuckDBGetUInt64(this)),

            DuckDBType.Float => ReadValue<float>(NativeMethods.Value.DuckDBGetFloat(this)),
            DuckDBType.Double => ReadValue<double>(NativeMethods.Value.DuckDBGetDouble(this)),

            //DuckDBType.Timestamp => ReadValue<T>(),
            //DuckDBType.Interval => ReadValue<T>(),
            //DuckDBType.Date => ReadValue<T>(),
            //DuckDBType.Time => ReadValue<T>(),
            //DuckDBType.TimeTz => ReadValue<T>(),
            //DuckDBType.HugeInt => ReadValue<DuckDBHugeInt>(NativeMethods.Value.DuckDBGetHugeInt(this)),
            //DuckDBType.UnsignedHugeInt => ReadValue<T>(),
            DuckDBType.Varchar => ReadValue<string>(NativeMethods.Value.DuckDBGetVarchar(this)),
            //DuckDBType.Decimal => ReadValue<T>(),
            //DuckDBType.Uuid => expr,
            _ => throw new NotImplementedException($"Cannot read value of type {typeof(T).FullName}")
        };

        T ReadValue<TSource>(TSource value)
        {
            return Unsafe.As<TSource, T>(ref value);
        }
    }
}
