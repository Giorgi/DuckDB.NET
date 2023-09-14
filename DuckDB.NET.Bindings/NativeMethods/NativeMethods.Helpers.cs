using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET;

public partial class NativeMethods
{
    public static class Helpers
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_free")]
        public static extern void DuckDBFree(IntPtr ptr);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_decimal_to_double")]
        public static extern double DuckDBDecimalToDouble(DuckDBDecimal val);
    }
}