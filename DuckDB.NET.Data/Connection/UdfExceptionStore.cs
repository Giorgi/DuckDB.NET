using System.Collections.Concurrent;

namespace DuckDB.NET.Data.Connection;

internal static class UdfExceptionStore
{
    private static readonly ConcurrentDictionary<ulong, Exception> Exceptions = new();

    // Nothing stores here any more: on C API V2 the callbacks of C# functions leave their exception with the connection
    // (DuckDBV2Connection.CallbackException). The V1 Arrow stream still reads it until stage 7 of docs/V2-MIGRATION.md.

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
}
