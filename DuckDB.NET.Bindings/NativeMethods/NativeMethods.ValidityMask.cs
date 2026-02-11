namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static partial class ValidityMask
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_validity_set_row_validity")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial void DuckDBValiditySetRowValidity(ulong* validity, ulong index, [MarshalAs(UnmanagedType.I1)] bool valid);
    }
}
