using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET;

public partial class NativeMethods
{
    public static class Types
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_boolean")]
        public static extern bool DuckDBValueBoolean([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int8")]
        public static extern sbyte DuckDBValueInt8([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int16")]
        public static extern short DuckDBValueInt16([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int32")]
        public static extern int DuckDBValueInt32([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int64")]
        public static extern long DuckDBValueInt64([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_decimal")]
        public static extern DuckDBDecimal DuckDBValueDecimal([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_uint8")]
        public static extern byte DuckDBValueUInt8([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_uint16")]
        public static extern ushort DuckDBValueUInt16([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_uint32")]
        public static extern uint DuckDBValueUInt32([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_uint64")]
        public static extern ulong DuckDBValueUInt64([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_float")]
        public static extern float DuckDBValueFloat([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_double")]
        public static extern double DuckDBValueDouble([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_interval")]
        public static extern DuckDBInterval DuckDBValueInterval([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_varchar")]
        public static extern IntPtr DuckDBValueVarchar([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_blob")]
        public static extern DuckDBBlob DuckDBValueBlob([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_date")]
        public static extern DuckDBDate DuckDBValueDate([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_time")]
        public static extern DuckDBTime DuckDBValueTime([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_timestamp")]
        public static extern DuckDBTimestampStruct DuckDBValueTimestamp([In, Out] ref DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_result_get_chunk")]
        public static extern DuckDBDataChunk DuckDBResultGetChunk([In, Out] DuckDBResult result, long chunkIndex);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_result_chunk_count")]
        public static extern long DuckDBResultChunkCount([In, Out] DuckDBResult result);
    }
}