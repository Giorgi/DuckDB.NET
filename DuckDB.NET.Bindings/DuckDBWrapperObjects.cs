using Microsoft.Win32.SafeHandles;
using System;

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
        var value = NativeMethods.Value.DuckDBGetInt32(this);
        return (T)(object)value;
        //return Unsafe.As<int, T>(ref value);
    }
}