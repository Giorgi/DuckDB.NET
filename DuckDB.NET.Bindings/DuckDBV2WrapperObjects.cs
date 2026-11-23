namespace DuckDB.NET.Native;

// SafeHandles for the handles of the C API V2. Every V2 destructor takes the handle by reference and sets it to null.

/// <summary>
/// The root of the V2 API. Its destructor refuses while an instance created through it is still alive, so an
/// environment is best kept for the lifetime of the process.
/// </summary>
public class DuckDBV2Environment() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        return NativeMethodsV2.Startup.DuckDBV2EnvironmentDestroy(ref handle) == DuckDBErrorCode.None;
    }
}

public class DuckDBV2Instance() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        return NativeMethodsV2.Startup.DuckDBV2InstanceDestroy(ref handle) == DuckDBErrorCode.None;
    }
}

public class DuckDBV2Connection() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        return NativeMethodsV2.Startup.DuckDBV2ConnectionDestroy(ref handle) == DuckDBErrorCode.None;
    }
}

public class DuckDBV2Result() : SafeHandleZeroOrMinusOneIsInvalid(true)
{
    protected override bool ReleaseHandle()
    {
        return NativeMethodsV2.Query.DuckDBV2ResultDestroy(ref handle) == DuckDBErrorCode.None;
    }
}
