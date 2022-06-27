using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET
{
    public class NativeMethods
    {
        private const string DuckDbLibrary = "duckdb";

        //Grouped according to https://duckdb.org/docs/api/c/overview

        public static class Startup
        {
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_open")]
            public static extern DuckDBState DuckDBOpen(string path, out DuckDBDatabase database);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_open_ext")]
            public static extern DuckDBState DuckDBOpen(string path, out DuckDBDatabase database, IntPtr config, out IntPtr error);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_close")]
            public static extern void DuckDBClose(out IntPtr database);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_connect")]
            public static extern DuckDBState DuckDBConnect(DuckDBDatabase database, out DuckDBNativeConnection connection);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_disconnect")]
            public static extern void DuckDBDisconnect(out IntPtr connection);
        }
        
        public static class Query
        {
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_query")]
            public static extern DuckDBState DuckDBQuery(DuckDBNativeConnection connection, string query, [In, Out] DuckDBResult result);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_query")]
            public static extern DuckDBState DuckDBQuery(DuckDBNativeConnection connection, SafeUnmanagedMemoryHandle query, [In, Out] DuckDBResult result);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_result")]
            public static extern void DuckDBDestroyResult([In, Out] DuckDBResult result);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_column_name")]
            public static extern IntPtr DuckDBColumnName([In, Out] DuckDBResult result, long col);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_column_type")]
            public static extern DuckDBType DuckDBColumnType([In, Out] DuckDBResult result, long col);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_column_count")]
            public static extern long DuckDBColumnCount([In, Out] DuckDBResult result);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_row_count")]
            public static extern long DuckDBRowCount([In, Out] DuckDBResult result);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_rows_changed")]
            public static extern long DuckDBRowsChanged([In, Out] DuckDBResult result);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_column_data")]
            public static extern IntPtr DuckDBColumnData([In, Out] DuckDBResult result, long col);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_nullmask_data")]
            public static extern IntPtr DuckDBNullmaskData([In, Out] DuckDBResult result, long col);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_result_error")]
            public static extern IntPtr DuckDBResultError([In, Out] DuckDBResult result);
        }
        
        public static class Types
        {
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_boolean")]
            public static extern bool DuckDBValueBoolean([In, Out] DuckDBResult result, long col, long row);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int8")]
            public static extern sbyte DuckDBValueInt8([In, Out] DuckDBResult result, long col, long row);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int16")]
            public static extern short DuckDBValueInt16([In, Out] DuckDBResult result, long col, long row);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int32")]
            public static extern int DuckDBValueInt32([In, Out] DuckDBResult result, long col, long row);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int64")]
            public static extern long DuckDBValueInt64([In, Out] DuckDBResult result, long col, long row);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_float")]
            public static extern float DuckDBValueFloat([In, Out] DuckDBResult result, long col, long row);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_double")]
            public static extern double DuckDBValueDouble([In, Out] DuckDBResult result, long col, long row);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_varchar")]
            public static extern IntPtr DuckDBValueVarchar([In, Out] DuckDBResult result, long col, long row);
        }

        public static class PreparedStatements
        {
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_prepare")]
            public static extern DuckDBState DuckDBPrepare(DuckDBNativeConnection connection, string query, out DuckDBPreparedStatement preparedStatement);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_nparams")]
            public static extern DuckDBState DuckDBParams(DuckDBPreparedStatement preparedStatement, out long numberOfParams);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_boolean")]
            public static extern DuckDBState DuckDBBindBoolean(DuckDBPreparedStatement preparedStatement, long index, bool val);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_int8")]
            public static extern DuckDBState DuckDBBindInt8(DuckDBPreparedStatement preparedStatement, long index, sbyte val);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_int16")]
            public static extern DuckDBState DuckDBBindInt16(DuckDBPreparedStatement preparedStatement, long index, short val);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_int32")]
            public static extern DuckDBState DuckDBBindInt32(DuckDBPreparedStatement preparedStatement, long index, int val);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_int64")]
            public static extern DuckDBState DuckDBBindInt64(DuckDBPreparedStatement preparedStatement, long index, long val);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_float")]
            public static extern DuckDBState DuckDBBindFloat(DuckDBPreparedStatement preparedStatement, long index, float val);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_double")]
            public static extern DuckDBState DuckDBBindDouble(DuckDBPreparedStatement preparedStatement, long index, double val);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_varchar")]
            public static extern DuckDBState DuckDBBindVarchar(DuckDBPreparedStatement preparedStatement, long index, string val);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_null")]
            public static extern DuckDBState DuckDBBindNull(DuckDBPreparedStatement preparedStatement, long index);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_execute_prepared")]
            public static extern DuckDBState DuckDBExecutePrepared(DuckDBPreparedStatement preparedStatement, [In, Out] DuckDBResult result);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_prepare")]
            public static extern void DuckDBDestroyPrepare(out IntPtr preparedStatement);
        }
        
        public static class Helpers
        {
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_free")]
            public static extern void DuckDBFree(IntPtr ptr);
        }
    }
}
