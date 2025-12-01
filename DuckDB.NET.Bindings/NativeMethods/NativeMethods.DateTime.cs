using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#datetimetimestamp-helpers
    public static class DateTimeHelpers
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_from_date")]
        public static extern DuckDBDateOnly DuckDBFromDate(DuckDBDate date);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_to_date")]
        public static extern DuckDBDate DuckDBToDate(DuckDBDateOnly dateStruct);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_from_time")]
        public static extern DuckDBTimeOnly DuckDBFromTime(DuckDBTime time);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_time_tz")]
        public static extern DuckDBTimeTzStruct DuckDBCreateTimeTz(long micros, int offset);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_from_time_tz")]
        public static extern DuckDBTimeTz DuckDBFromTimeTz(DuckDBTimeTzStruct micros);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_to_time")]
        public static extern DuckDBTime DuckDBToTime(DuckDBTimeOnly dateStruct);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_from_timestamp")]
        public static extern DuckDBTimestamp DuckDBFromTimestamp(DuckDBTimestampStruct date);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_to_timestamp")]
        public static extern DuckDBTimestampStruct DuckDBToTimestamp(DuckDBTimestamp dateStruct);

        // NOTE: for boolean return values, MarshalAs(UnmanagedType.I1) is used because the default is to use 4-byte Win32 BOOLs
        // https://learn.microsoft.com/en-us/dotnet/standard/native-interop/customize-struct-marshalling#customizing-boolean-field-marshalling
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_is_finite_date")]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool DuckDBIsFiniteDate(DuckDBDate date);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_is_finite_timestamp")]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool DuckDBIsFiniteTimestamp(DuckDBTimestampStruct ts);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_is_finite_timestamp_s")]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool DuckDBIsFiniteTimestampS(DuckDBTimestampStruct ts);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_is_finite_timestamp_ms")]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool DuckDBIsFiniteTimestampMs(DuckDBTimestampStruct ts);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_is_finite_timestamp_ns")]
        [return: MarshalAs(UnmanagedType.I1)]
        public static extern bool DuckDBIsFiniteTimestampNs(DuckDBTimestampStruct ts);
    }
}
