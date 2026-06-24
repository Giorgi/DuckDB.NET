namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/stable/clients/c/api#error-data
    public static partial class ErrorData
    {
        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_error_data_has_error")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalAs(UnmanagedType.I1)]
        public static partial bool DuckDBErrorDataHasError(DuckDBErrorData errorData);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_error_data_message")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalUsing(typeof(DuckDBOwnedStringMarshaller))]
        public static partial string DuckDBErrorDataMessage(DuckDBErrorData errorData);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_error_data")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyErrorData(ref IntPtr errorData);
    }
}
