namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static partial class Helpers
    {
        // Maybe [SuppressGCTransition]: free() — typically fast, but can call munmap for large allocations
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_free")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBFree(IntPtr ptr);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_decimal_to_double")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial double DuckDBDecimalToDouble(DuckDBDecimal val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_vector_size")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial ulong DuckDBVectorSize();
    }
}
