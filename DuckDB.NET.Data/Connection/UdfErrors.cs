using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data.Connection;

// What the registration of functions (Functions/) does with the exception that a function throws, besides giving
// DuckDB its message: DuckDB.NET keeps it, so that the command that ran the query throws a DuckDBException with it as
// the InnerException (UdfExceptionStore, keyed by the ID of the connection). This file is not in Functions/ on
// purpose: DuckDB.ExtensionKit compiles that folder with a UdfErrors of its own, with the same methods, which does
// nothing. An extension has no .NET caller to hand an exception to, and the client context functions used here are
// not in the C API it targets.
internal static class UdfErrors
{
    // Called while a scalar function is registered, before DuckDB is given its callback.
    public static unsafe void ConfigureScalarFunction(IntPtr function)
    {
        NativeMethods.ScalarFunction.DuckDBScalarFunctionSetBind(function, &ScalarFunctionBind);
    }

    // Called when a scalar function throws, before DuckDB is given the message.
    public static void ScalarFunctionFailed(IntPtr info, Exception exception)
    {
        try
        {
            var bindDataHandle = GCHandle.FromIntPtr(NativeMethods.ScalarFunction.DuckDBScalarFunctionGetBindData(info));
            if (bindDataHandle.Target is ulong connectionId)
            {
                UdfExceptionStore.Store(connectionId, exception);
            }
        }
        catch
        {
            // If we can't get the connection ID, we still report the error message
        }
    }

    // Called when a table function is bound. What it returns is kept in the bind data of the function and passed to
    // TableFunctionFailed: the ID of the connection.
    public static ulong TableFunctionBound(IntPtr bindInfo) => UdfExceptionStore.GetTableFunctionBindConnectionId(bindInfo);

    // Called when the bind of a table function throws, before DuckDB is given the message.
    public static void TableFunctionBindFailed(IntPtr bindInfo, Exception exception)
    {
        try
        {
            var connectionId = UdfExceptionStore.GetTableFunctionBindConnectionId(bindInfo);
            UdfExceptionStore.Store(connectionId, exception);
        }
        catch
        {
            // If we can't get the connection ID, we still report the error message
        }
    }

    // Called when the init or a scan of a table function throws, with what TableFunctionBound returned.
    public static void TableFunctionFailed(ulong connectionId, Exception exception) => UdfExceptionStore.Store(connectionId, exception);

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static unsafe void ScalarFunctionBind(IntPtr info)
    {
        try
        {
            var connectionId = UdfExceptionStore.GetScalarFunctionBindConnectionId(info);
            NativeMethods.ScalarFunction.DuckDBScalarFunctionSetBindData(info, connectionId.ToHandle(), &DestroyBindData);

            // DuckDB copies a bound expression, for a filter in WHERE or HAVING for example, and without this
            // callback the copy has no bind data. The function then cannot tell which connection it runs on, and
            // an exception it throws reaches the caller without its InnerException.
            NativeMethods.ScalarFunction.DuckDBScalarFunctionSetBindDataCopy(info, &CopyBindData);
        }
        catch
        {
            // If we can't get the connection ID, we still allow the function to proceed
        }
    }

    // Each copy of the bind data is destroyed by itself, so each gets a handle of its own to the connection ID.
    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static IntPtr CopyBindData(IntPtr bindData)
    {
        try
        {
            return bindData != IntPtr.Zero && GCHandle.FromIntPtr(bindData).Target is { } connectionId ? connectionId.ToHandle() : IntPtr.Zero;
        }
        catch
        {
            // Without bind data the function still runs; only the InnerException is lost.
            return IntPtr.Zero;
        }
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void DestroyBindData(IntPtr pointer) => pointer.FreeHandle();
}
