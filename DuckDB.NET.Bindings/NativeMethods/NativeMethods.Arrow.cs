using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static class Arrow
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_query_arrow")]
        public static extern DuckDBState DuckDBQueryArrow(DuckDBNativeConnection connection, SafeUnmanagedMemoryHandle query, out DuckDBArrow result);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_query_arrow_schema")]
        public static extern DuckDBState DuckDBQueryArrowSchema(DuckDBNativeConnection connection, out DuckDBArrowSchema schema);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_prepared_arrow_schema")]
        public static extern DuckDBState DuckDBPreparedArrowSchema(DuckDBPreparedStatement preparedStatement, out DuckDBArrowSchema schema);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_result_arrow_array")]
        public static extern DuckDBState DuckDBResultArrowArray(DuckDBResult result, DuckDBDataChunk chunk, out DuckDBArrowArray array);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_query_arrow_array")]
        public static extern DuckDBState DuckDBQueryArrowArray(DuckDBArrow result, out DuckDBArrowArray array);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_arrow_column_count")]
        public static extern long DuckDBArrowColumnCount(DuckDBArrow result);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_arrow_row_count")]
        public static extern long DuckDBArrowRowCount(DuckDBArrow result);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_arrow_rows_changed")]
        public static extern long DuckDBArrowRowsChanged(DuckDBArrow result);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_query_arrow_error")]
        public static extern IntPtr DuckDBQueryArrowError(DuckDBArrow result);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_arrow")]
        public static extern DuckDBState DuckDBDestroyArrow(out IntPtr result);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_arrow_stream")]
        public static extern DuckDBState DuckDBDestroyArrowStream(out IntPtr stream_p);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_execute_prepared_arrow")]
        public static extern DuckDBState DuckDBExecutePreparedArrow(DuckDBPreparedStatement preparedStatement, out DuckDBArrow result);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_arrow_scan")]
        public static extern DuckDBState DuckDBArrowScan(DuckDBNativeConnection connection, SafeUnmanagedMemoryHandle tableName, DuckDBArrowStream arrow);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_arrow_array_scan")]
        public static extern DuckDBState DuckDBArrowArrayScan(
            DuckDBNativeConnection connection,
            SafeUnmanagedMemoryHandle tableName,
            DuckDBArrowSchema arrowSchema,
            DuckDBArrowArray arrowArray,
            out DuckDBArrowStream stream
        );
    }
}
