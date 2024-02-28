using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static class DateTime
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_from_date")]
        public static extern DuckDBDateOnly DuckDBFromDate(DuckDBDate date);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_to_date")]
        public static extern DuckDBDate DuckDBToDate(DuckDBDateOnly dateStruct);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_from_time")]
        public static extern DuckDBTimeOnly DuckDBFromTime(DuckDBTime date);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_to_time")]
        public static extern DuckDBTime DuckDBToTime(DuckDBTimeOnly dateStruct);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_from_timestamp")]
        public static extern DuckDBTimestamp DuckDBFromTimestamp(DuckDBTimestampStruct date);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_to_timestamp")]
        public static extern DuckDBTimestampStruct DuckDBToTimestamp(DuckDBTimestamp dateStruct);
    }
}