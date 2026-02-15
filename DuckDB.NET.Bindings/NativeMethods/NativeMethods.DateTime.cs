namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#datetimetimestamp-helpers
    public static partial class DateTimeHelpers
    {
        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_from_date")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBDateOnly DuckDBFromDate(DuckDBDate date);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_to_date")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBDate DuckDBToDate(DuckDBDateOnly dateStruct);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_from_time")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBTimeOnly DuckDBFromTime(DuckDBTime time);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_time_tz")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBTimeTzStruct DuckDBCreateTimeTz(long micros, int offset);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_from_time_tz")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBTimeTz DuckDBFromTimeTz(DuckDBTimeTzStruct micros);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_to_time")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBTime DuckDBToTime(DuckDBTimeOnly dateStruct);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_from_timestamp")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBTimestamp DuckDBFromTimestamp(DuckDBTimestampStruct date);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_to_timestamp")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBTimestampStruct DuckDBToTimestamp(DuckDBTimestamp dateStruct);

        // NOTE: for boolean return values, MarshalAs(UnmanagedType.I1) is used because the default is to use 4-byte Win32 BOOLs
        // https://learn.microsoft.com/en-us/dotnet/standard/native-interop/customize-struct-marshalling#customizing-boolean-field-marshalling
        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_is_finite_date")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalAs(UnmanagedType.I1)]
        public static partial bool DuckDBIsFiniteDate(DuckDBDate date);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_is_finite_timestamp")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalAs(UnmanagedType.I1)]
        public static partial bool DuckDBIsFiniteTimestamp(DuckDBTimestampStruct ts);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_is_finite_timestamp_s")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalAs(UnmanagedType.I1)]
        public static partial bool DuckDBIsFiniteTimestampS(DuckDBTimestampStruct ts);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_is_finite_timestamp_ms")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalAs(UnmanagedType.I1)]
        public static partial bool DuckDBIsFiniteTimestampMs(DuckDBTimestampStruct ts);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_is_finite_timestamp_ns")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalAs(UnmanagedType.I1)]
        public static partial bool DuckDBIsFiniteTimestampNs(DuckDBTimestampStruct ts);
    }
}
