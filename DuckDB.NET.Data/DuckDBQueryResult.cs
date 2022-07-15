using System;

namespace DuckDB.NET.Data;

internal sealed class DuckDBQueryResult : IDisposable
{
    private bool disposed = false;
    public DuckDBResult NativeHandle { get; }

    public DuckDBQueryResult(DuckDBResult duckDbResult)
    {
        NativeHandle = duckDbResult;
    }

    public void Dispose()
    {
        if (disposed)
            throw new ObjectDisposedException(nameof(DuckDBQueryResult));
        
        NativeMethods.Query.DuckDBDestroyResult(NativeHandle);
        disposed = true;
    }
}