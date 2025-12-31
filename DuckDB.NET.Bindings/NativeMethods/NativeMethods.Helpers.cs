namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static class Helpers
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_free")]
        public static extern void DuckDBFree(IntPtr ptr);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_decimal_to_double")]
        public static extern double DuckDBDecimalToDouble(DuckDBDecimal val);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_vector_size")]
        public static extern ulong DuckDBVectorSize();
    }
}