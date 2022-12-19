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
            public static extern DuckDBState DuckDBOpen(string path, out DuckDBDatabase database, DuckDBConfig config, out IntPtr error);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_close")]
            public static extern void DuckDBClose(out IntPtr database);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_connect")]
            public static extern DuckDBState DuckDBConnect(DuckDBDatabase database, out DuckDBNativeConnection connection);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_disconnect")]
            public static extern void DuckDBDisconnect(out IntPtr connection);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_library_version")]
            public static extern IntPtr DuckDBLibraryVersion();
        }

        public static class Configure
        {
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_config")]
            public static extern DuckDBState DuckDBCreateConfig(out DuckDBConfig config);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_config_count")]
            public static extern int DuckDBConfigCount();

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_config_flag")]
            public static extern DuckDBState DuckDBGetConfigFlag(int index, out IntPtr name, out IntPtr description);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_set_config")]
            public static extern DuckDBState DuckDBSetConfig(DuckDBConfig config, string name, string option);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_config")]
            public static extern void DuckDBDestroyConfig(out IntPtr config);
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

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_decimal")]
            public static extern DuckDBDecimal DuckDBValueDecimal([In, Out] DuckDBResult result, long col, long row);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_uint8")]
            public static extern byte DuckDBValueUInt8([In, Out] DuckDBResult result, long col, long row);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_uint16")]
            public static extern ushort DuckDBValueUInt16([In, Out] DuckDBResult result, long col, long row);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_uint32")]
            public static extern uint DuckDBValueUInt32([In, Out] DuckDBResult result, long col, long row);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_uint64")]
            public static extern ulong DuckDBValueUInt64([In, Out] DuckDBResult result, long col, long row);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_float")]
            public static extern float DuckDBValueFloat([In, Out] DuckDBResult result, long col, long row);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_double")]
            public static extern double DuckDBValueDouble([In, Out] DuckDBResult result, long col, long row);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_interval")]
            public static extern DuckDBInterval DuckDBValueInterval([In, Out] DuckDBResult result, long col, long row);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_varchar")]
            public static extern IntPtr DuckDBValueVarchar([In, Out] DuckDBResult result, long col, long row);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_blob")]
            public static extern DuckDBBlob DuckDBValueBlob([In, Out] DuckDBResult result, long col, long row);
            
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_date")]
            public static extern DuckDBDate DuckDBValueDate([In, Out] DuckDBResult result, long col, long row);
            
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_time")]
            public static extern DuckDBTime DuckDBValueTime([In, Out] DuckDBResult result, long col, long row);
            
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_timestamp")]
            public static extern DuckDBTimestampStruct DuckDBValueTimestamp([In, Out] DuckDBResult result, long col, long row);
        }

        public static class PreparedStatements
        {
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_prepare")]
            public static extern DuckDBState DuckDBPrepare(DuckDBNativeConnection connection, string query, out DuckDBPreparedStatement preparedStatement);
            
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_prepare")]
            public static extern DuckDBState DuckDBPrepare(DuckDBNativeConnection connection, SafeUnmanagedMemoryHandle query, out DuckDBPreparedStatement preparedStatement);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_prepare")]
            public static extern void DuckDBDestroyPrepare(out IntPtr preparedStatement);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_prepare_error")]
            public static extern IntPtr DuckDBPrepareError(DuckDBPreparedStatement preparedStatement);
            
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_nparams")]
            public static extern long DuckDBParams(DuckDBPreparedStatement preparedStatement);

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

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_hugeint")]
            public static extern DuckDBState DuckDBBindHugeInt(DuckDBPreparedStatement preparedStatement, long index, DuckDBHugeInt val);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_uint8")]
            public static extern DuckDBState DuckDBBindUInt8(DuckDBPreparedStatement preparedStatement, long index, byte val);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_uint16")]
            public static extern DuckDBState DuckDBBindUInt16(DuckDBPreparedStatement preparedStatement, long index, ushort val);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_uint32")]
            public static extern DuckDBState DuckDBBindUInt32(DuckDBPreparedStatement preparedStatement, long index, uint val);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_uint64")]
            public static extern DuckDBState DuckDBBindUInt64(DuckDBPreparedStatement preparedStatement, long index, ulong val);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_float")]
            public static extern DuckDBState DuckDBBindFloat(DuckDBPreparedStatement preparedStatement, long index, float val);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_double")]
            public static extern DuckDBState DuckDBBindDouble(DuckDBPreparedStatement preparedStatement, long index, double val);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_varchar")]
            public static extern DuckDBState DuckDBBindVarchar(DuckDBPreparedStatement preparedStatement, long index, string val);
            
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_varchar")]
            public static extern DuckDBState DuckDBBindVarchar(DuckDBPreparedStatement preparedStatement, long index, SafeUnmanagedMemoryHandle val);
            
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_blob")]
            public static extern DuckDBState DuckDBBindBlob(DuckDBPreparedStatement preparedStatement, long index, [In] byte[] data, long length);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_null")]
            public static extern DuckDBState DuckDBBindNull(DuckDBPreparedStatement preparedStatement, long index);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_date")]
            public static extern DuckDBState DuckDBBindDate(DuckDBPreparedStatement preparedStatement, long index, DuckDBDate val);
            
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_time")]
            public static extern DuckDBState DuckDBBindTime(DuckDBPreparedStatement preparedStatement, long index, DuckDBTime val);
            
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_timestamp")]
            public static extern DuckDBState DuckDBBindTimestamp(DuckDBPreparedStatement preparedStatement, long index, DuckDBTimestampStruct val);
            
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_execute_prepared")]
            public static extern DuckDBState DuckDBExecutePrepared(DuckDBPreparedStatement preparedStatement, [In, Out] DuckDBResult result);
        }

        public static class Appender
        {
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_create")]
            public static extern DuckDBState DuckDBAppenderCreate(DuckDBNativeConnection connection, string schema, string table, out DuckDBAppender appender);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_error")]
            public static extern IntPtr DuckDBAppenderError(DuckDBAppender appender);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_flush")]
            public static extern DuckDBState DuckDBAppenderFlush(DuckDBAppender appender);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_end_row")]
            public static extern DuckDBState DuckDBAppenderEndRow(DuckDBAppender appender);
            
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_close")]
            public static extern DuckDBState DuckDBAppenderClose(DuckDBAppender appender);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_destroy")]
            public static extern DuckDBState DuckDBDestroyAppender(out IntPtr appender);

            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_bool")]
            #if NET5_0_OR_GREATER
            [SuppressGCTransition]
            #endif
            public static extern DuckDBState DuckDBAppendBool(DuckDBAppender appender, bool val);
#if NET5_0_OR_GREATER
            [SuppressGCTransition]
#endif
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_int8")]
            public static extern DuckDBState DuckDBAppendInt8(DuckDBAppender appender, sbyte val);
#if NET5_0_OR_GREATER
            [SuppressGCTransition]
#endif
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_int16")]
            public static extern DuckDBState DuckDBAppendInt16(DuckDBAppender appender, short val);
#if NET5_0_OR_GREATER
            [SuppressGCTransition]
#endif
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_int32")]
            public static extern DuckDBState DuckDBAppendInt32(DuckDBAppender appender, int val);
#if NET5_0_OR_GREATER
            [SuppressGCTransition]
#endif
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_int64")]
            public static extern DuckDBState DuckDBAppendInt64(DuckDBAppender appender, long val);
#if NET5_0_OR_GREATER
            [SuppressGCTransition]
#endif
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_uint8")]
            public static extern DuckDBState DuckDBAppendUInt8(DuckDBAppender appender, byte val);
#if NET5_0_OR_GREATER
            [SuppressGCTransition]
#endif
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_uint16")]
            public static extern DuckDBState DuckDBAppendUInt16(DuckDBAppender appender, ushort val);
#if NET5_0_OR_GREATER
            [SuppressGCTransition]
#endif
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_uint32")]
            public static extern DuckDBState DuckDBAppendUInt32(DuckDBAppender appender, uint val);
#if NET5_0_OR_GREATER
            [SuppressGCTransition]
#endif
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_uint64")]
            public static extern DuckDBState DuckDBAppendUInt64(DuckDBAppender appender, ulong val);
#if NET5_0_OR_GREATER
            [SuppressGCTransition]
#endif
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_float")]
            public static extern DuckDBState DuckDBAppendFloat(DuckDBAppender appender, float val);
#if NET5_0_OR_GREATER
            [SuppressGCTransition]
#endif
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_double")]
            public static extern DuckDBState DuckDBAppendDouble(DuckDBAppender appender, double val);
#if NET5_0_OR_GREATER
            [SuppressGCTransition]
#endif
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_varchar")]
            public static extern DuckDBState DuckDBAppendVarchar(DuckDBAppender appender, string val);
#if NET5_0_OR_GREATER
            [SuppressGCTransition]
#endif
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_timestamp")]
            public static extern DuckDBState DuckDBAppendTimestamp(DuckDBAppender appender, DuckDBTimestamp val);
#if NET5_0_OR_GREATER
            [SuppressGCTransition]
#endif
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_date")]
            public static extern DuckDBState DuckDBAppendDate(DuckDBAppender appender, DuckDBDate val);
#if NET5_0_OR_GREATER
            [SuppressGCTransition]
#endif
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_time")]
            public static extern DuckDBState DuckDBAppendTime(DuckDBAppender appender, DuckDBTime val);
#if NET5_0_OR_GREATER
            [SuppressGCTransition]
#endif
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_null")]
            public static extern DuckDBState DuckDBAppendNull(DuckDBAppender appender);
        }

        public static class Helpers
        {
            [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_free")]
            public static extern void DuckDBFree(IntPtr ptr);
        }

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
}
