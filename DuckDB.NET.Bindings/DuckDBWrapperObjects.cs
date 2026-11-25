namespace DuckDB.NET.Native;

public class DuckDBDatabase() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        NativeMethods.Startup.DuckDBClose(ref handle);
        return true;
    }
}

public class DuckDBNativeConnection() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        NativeMethods.Startup.DuckDBDisconnect(ref handle);
        return true;
    }
}

public class DuckDBPreparedStatement() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        NativeMethods.PreparedStatements.DuckDBDestroyPrepare(ref handle);
        return true;
    }
}

public class DuckDBAppender() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        return NativeMethods.Appender.DuckDBDestroyAppender(ref handle).IsSuccess();
    }
}

public class DuckDBArrowConvertedSchema() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        NativeMethods.Arrow.DuckDBDestroyArrowConvertedSchema(ref handle);
        return true;
    }
}

public class DuckDBErrorData() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    public bool HasError => !IsInvalid && NativeMethods.ErrorData.DuckDBErrorDataHasError(this);

    public string? Message => IsInvalid ? null : NativeMethods.ErrorData.DuckDBErrorDataMessage(this);

    protected override bool ReleaseHandle()
    {
        NativeMethods.ErrorData.DuckDBDestroyErrorData(ref handle);
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
        NativeMethods.DataChunks.DuckDBDestroyDataChunk(ref handle);
        return true;
    }
}

