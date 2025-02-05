using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Common;

internal static class DuckDBGlobalData
{
    public static ulong VectorSize { get; } = NativeMethods.Helpers.DuckDBVectorSize();
}