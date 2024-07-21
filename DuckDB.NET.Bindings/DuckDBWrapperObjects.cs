using Microsoft.Win32.SafeHandles;

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
        NativeMethods.Startup.DuckDBInterrupt(handle);
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

public class DuckDBDataChunk() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        NativeMethods.DataChunks.DuckDBDestroyDataChunk(out handle);
        return true;
    }
}