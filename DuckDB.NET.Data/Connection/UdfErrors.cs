using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using DuckDB.NET.Data.Execution;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Connection;

// What the registration of functions (Functions/) does with the exception that a function throws, besides giving
// DuckDB its message: DuckDB.NET keeps it on the native connection that ran the statement
// (DuckDBV2Connection.CallbackException), so that the command throws a DuckDBException with it as the InnerException.
// The connection is found while the function is bound, which runs on the thread of the statement (DuckDBV2CallbackScope),
// and kept in the bind data for the callbacks on DuckDB's worker threads (docs/V2-MIGRATION.md, stages 5 and 6).
internal static class UdfErrors
{
    // Called while a scalar function is registered, before DuckDB is given its callback.
    public static unsafe void ConfigureScalarFunction(_duckdb_v2_scalar_function* function)
    {
        _duckdb_v2_error_info* error = null;
        NativeMethodsV2.duckdb_v2_scalar_function_set_bind_callback(function, &ScalarFunctionBind, &error).ThrowIfFailed(ref error);
    }

    // Called when a scalar function throws, before DuckDB is given the message.
    public static unsafe void ScalarFunctionFailed(_duckdb_v2_scalar_function_exec_info* info, Exception exception)
    {
        try
        {
            void* bindData;
            if (NativeMethodsV2.duckdb_v2_scalar_function_exec_get_bind_data(info, &bindData, null) == DUCKDB_V2_ERROR.NONE && bindData != null &&
                GCHandle.FromIntPtr((IntPtr)bindData).Target is DuckDBV2Connection connection)
            {
                connection.CallbackException = exception;
            }
        }
        catch
        {
            // Without the connection we still report the error message
        }
    }

    // Called when a table function is bound. What it returns is kept in the bind data of the function and passed to
    // TableFunctionFailed: the connection that runs the statement, or null outside DuckDBV2CallbackScope.
    public static DuckDBV2Connection? TableFunctionBound() => DuckDBV2CallbackScope.Current;

    // Called when the bind, the init or a scan of a table function throws, with what TableFunctionBound returned.
    public static void TableFunctionFailed(DuckDBV2Connection? connection, Exception exception)
    {
        if (connection is not null)
        {
            connection.CallbackException = exception;
        }
    }

    // The connection becomes the bind data, so the exec callbacks can leave an exception with it. DuckDB shares the
    // bind data between the copies it makes of a bound expression, for a filter in WHERE or HAVING for example.
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void ScalarFunctionBind(_duckdb_v2_function_bind_info* info, _duckdb_v2_scalar_function_bind_info* result,
                                                  _duckdb_v2_context* context, _duckdb_v2_error_info** err)
    {
        try
        {
            // Without a connection the function still runs; only the InnerException is lost.
            if (DuckDBV2CallbackScope.Current is not { } connection)
            {
                return;
            }

            var bindData = new duckdb_v2_opaque { ptr = (void*)GCHandle.ToIntPtr(GCHandle.Alloc(connection)), destroy = &FreeHandle };
            if (NativeMethodsV2.duckdb_v2_scalar_function_bind_set_bind_data(result, &bindData, null) != DUCKDB_V2_ERROR.NONE)
            {
                GCHandle.FromIntPtr((IntPtr)bindData.ptr).Free();
            }
        }
        catch
        {
            // An exception must not leave a callback; the function runs without the bind data.
        }
    }

    // The bind data: the connection, which only the handle is freed for.
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void FreeHandle(void* pointer) => GCHandle.FromIntPtr((IntPtr)pointer).Free();
}
