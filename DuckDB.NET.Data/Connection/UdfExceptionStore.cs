using System.Collections.Concurrent;

namespace DuckDB.NET.Data.Connection;

internal static class UdfExceptionStore
{
    private static readonly ConcurrentDictionary<ulong, Exception> Exceptions = new();

    internal static void Store(ulong connectionId, Exception exception) => Exceptions[connectionId] = exception;

    internal static Exception? Retrieve(DuckDBNativeConnection nativeConnection)
    {
        var connectionId = GetConnectionId(nativeConnection);
        _ = Exceptions.TryRemove(connectionId, out var exception);
        return exception;
    }

    private static ulong GetConnectionId(DuckDBNativeConnection nativeConnection)
    {
        NativeMethods.Startup.DuckDBConnectionGetClientContext(nativeConnection, out var context);
        using (context)
        {
            return context.ConnectionId;
        }
    }

    internal static ulong GetBindConnectionId(IntPtr bindInfo)
    {
        NativeMethods.TableFunction.DuckDBTableFunctionGetClientContext(bindInfo, out var context);
        using (context)
        {
            return context.ConnectionId;
        }
    }
}
