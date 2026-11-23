namespace DuckDB.NET.Native;

public partial class NativeMethodsV2
{
    // The error info that a failed call leaves in its err slot. Its text is borrowed from it.
    public static unsafe partial class ErrorInfo
    {
        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_v2_error_info_get_code")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorCode DuckDBV2ErrorInfoGetCode(IntPtr info, out DuckDBErrorCode code);

        // The full text, "<Type> Error: <message>". Not null-terminated; valid until the info is destroyed.
        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_v2_error_info_get_text")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorCode DuckDBV2ErrorInfoGetText(IntPtr info, out byte* text, out ulong textLength);

        // The text without its "<Type> Error: " prefix.
        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_v2_error_info_get_raw_text")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorCode DuckDBV2ErrorInfoGetRawText(IntPtr info, out byte* text, out ulong textLength);

        // For callbacks, which report a failure by filling the err slot they receive.
        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_v2_error_info_set_code")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorCode DuckDBV2ErrorInfoSetCode(IntPtr info, DuckDBErrorCode code);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_v2_error_info_set_text")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorCode DuckDBV2ErrorInfoSetText(IntPtr info, byte* text, ulong textLength);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_v2_error_info_destroy")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBErrorCode DuckDBV2ErrorInfoDestroy(ref IntPtr info);
    }
}
