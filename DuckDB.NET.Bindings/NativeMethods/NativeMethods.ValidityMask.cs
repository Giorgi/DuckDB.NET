namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static class ValidityMask
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_validity_set_row_validity")]
        public static extern unsafe void DuckDBValiditySetRowValidity(ulong* validity, ulong index, bool valid);
    }
}