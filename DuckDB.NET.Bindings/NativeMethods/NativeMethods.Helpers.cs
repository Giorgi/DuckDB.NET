namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static partial class Helpers
    {
        // Maybe [SuppressGCTransition]: free() — typically fast, but can call munmap for large allocations
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_free")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBFree(IntPtr ptr);
    }
}
