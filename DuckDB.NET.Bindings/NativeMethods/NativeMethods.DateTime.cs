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
    }
}