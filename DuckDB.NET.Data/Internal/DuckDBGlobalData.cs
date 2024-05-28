using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal;

public static class DuckDBGlobalData
{
    public static ulong VectorSize { get; } = NativeMethods.Helpers.DuckDBVectorSize();
}