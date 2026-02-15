namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static partial class Helpers
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_free")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBFree(IntPtr ptr);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_decimal_to_double")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial double DuckDBDecimalToDouble(DuckDBDecimal val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_vector_size")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial ulong DuckDBVectorSize();
    }
}
