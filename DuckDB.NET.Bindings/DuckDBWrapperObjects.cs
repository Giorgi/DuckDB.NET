﻿using Microsoft.Win32.SafeHandles;

namespace DuckDB.NET.Native;

public class DuckDBDatabase : SafeHandleZeroOrMinusOneIsInvalid
{
    public DuckDBDatabase() : base(true)
    {
    }

    protected override bool ReleaseHandle()
    {
        NativeMethods.Startup.DuckDBClose(out handle);
        return true;
    }
}

public class DuckDBNativeConnection : SafeHandleZeroOrMinusOneIsInvalid
{
    public DuckDBNativeConnection() : base(true)
    {
    }

    protected override bool ReleaseHandle()
    {
        NativeMethods.Startup.DuckDBDisconnect(out handle);
        return true;
    }
}

public class DuckDBPreparedStatement : SafeHandleZeroOrMinusOneIsInvalid
{
    public DuckDBPreparedStatement() : base(true)
    {
    }

    protected override bool ReleaseHandle()
    {
        NativeMethods.PreparedStatements.DuckDBDestroyPrepare(out handle);
        return true;
    }
}

public class DuckDBConfig : SafeHandleZeroOrMinusOneIsInvalid
{
    public DuckDBConfig() : base(true)
    {

    }

    protected override bool ReleaseHandle()
    {
        NativeMethods.Configure.DuckDBDestroyConfig(out handle);
        return true;
    }
}

public class DuckDBAppender : SafeHandleZeroOrMinusOneIsInvalid
{
    public DuckDBAppender() : base(true)
    {
    }

    protected override bool ReleaseHandle()
    {
        return NativeMethods.Appender.DuckDBDestroyAppender(out handle) == DuckDBState.Success;
    }
}

public class DuckDBExtractedStatements : SafeHandleZeroOrMinusOneIsInvalid
{
    public DuckDBExtractedStatements() : base(true)
    {
    }

    protected override bool ReleaseHandle()
    {
        NativeMethods.ExtractStatements.DuckDBDestroyExtracted(out handle);

        return true;
    }
}

public class DuckDBLogicalType : SafeHandleZeroOrMinusOneIsInvalid
{
    public DuckDBLogicalType() : base(true)
    {
    }

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

    protected override bool ReleaseHandle()
    {
        NativeMethods.DataChunks.DuckDBDestroyDataChunk(out handle);
        return true;
    }
}

public class DuckDBArrow : SafeHandleZeroOrMinusOneIsInvalid
{
    public DuckDBArrow() : base(true)
    {
    }

    protected override bool ReleaseHandle()
    {
        NativeMethods.Arrow.DuckDBDestroyArrow(out handle);
        return true;
    }
}

public class DuckDBArrowStream : SafeHandleZeroOrMinusOneIsInvalid
{
    public DuckDBArrowStream() : base(true)
    {
    }

    protected override bool ReleaseHandle()
    {
        NativeMethods.Arrow.DuckDBDestroyArrowStream(out handle);
        return true;
    }
}