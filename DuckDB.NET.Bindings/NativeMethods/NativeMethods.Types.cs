namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#safe-fetch-functions
    public static partial class Types
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_int32")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial int DuckDBValueInt32(ref DuckDBResult result, long col, long row);
    }
}
