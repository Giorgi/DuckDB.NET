namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/appender
    public static class Appender
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_create")]
        public static extern DuckDBState DuckDBAppenderCreate(DuckDBNativeConnection connection, string? schema, string table, out DuckDBAppender appender);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_create")]
        public static extern DuckDBState DuckDBAppenderCreate(DuckDBNativeConnection connection, SafeUnmanagedMemoryHandle schema, SafeUnmanagedMemoryHandle table, out DuckDBAppender appender);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_create_ext")]
        public static extern DuckDBState DuckDBAppenderCreateExt(DuckDBNativeConnection connection, SafeUnmanagedMemoryHandle catalog, SafeUnmanagedMemoryHandle schema, SafeUnmanagedMemoryHandle table, out DuckDBAppender appender);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_column_count")]
        public static extern ulong DuckDBAppenderColumnCount(DuckDBAppender appender);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_column_type")]
        public static extern DuckDBLogicalType DuckDBAppenderColumnType(DuckDBAppender appender, ulong index);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_error")]
        public static extern IntPtr DuckDBAppenderError(DuckDBAppender appender);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_flush")]
        public static extern DuckDBState DuckDBAppenderFlush(DuckDBAppender appender);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_clear")]
        public static extern DuckDBState DuckDBAppenderClear(DuckDBAppender appender);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_end_row")]
        public static extern DuckDBState DuckDBAppenderEndRow(DuckDBAppender appender);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_close")]
        public static extern DuckDBState DuckDBAppenderClose(DuckDBAppender appender);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_destroy")]
        public static extern DuckDBState DuckDBDestroyAppender(ref IntPtr appender);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_bool")]
        public static extern DuckDBState DuckDBAppendBool(DuckDBAppender appender, bool val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_int8")]
        public static extern DuckDBState DuckDBAppendInt8(DuckDBAppender appender, sbyte val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_int16")]
        public static extern DuckDBState DuckDBAppendInt16(DuckDBAppender appender, short val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_int32")]
        public static extern DuckDBState DuckDBAppendInt32(DuckDBAppender appender, int val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_int64")]
        public static extern DuckDBState DuckDBAppendInt64(DuckDBAppender appender, long val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_hugeint")]
        public static extern DuckDBState DuckDBAppendHugeInt(DuckDBAppender appender, DuckDBHugeInt val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_uhugeint")]
        public static extern DuckDBState DuckDBAppendUHugeInt(DuckDBAppender appender, DuckDBUHugeInt val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_uint8")]
        public static extern DuckDBState DuckDBAppendUInt8(DuckDBAppender appender, byte val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_uint16")]
        public static extern DuckDBState DuckDBAppendUInt16(DuckDBAppender appender, ushort val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_uint32")]
        public static extern DuckDBState DuckDBAppendUInt32(DuckDBAppender appender, uint val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_uint64")]
        public static extern DuckDBState DuckDBAppendUInt64(DuckDBAppender appender, ulong val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_float")]
        public static extern DuckDBState DuckDBAppendFloat(DuckDBAppender appender, float val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_double")]
        public static extern DuckDBState DuckDBAppendDouble(DuckDBAppender appender, double val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_date")]
        public static extern DuckDBState DuckDBAppendDate(DuckDBAppender appender, DuckDBDate val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_time")]
        public static extern DuckDBState DuckDBAppendTime(DuckDBAppender appender, DuckDBTime val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_timestamp")]
        public static extern DuckDBState DuckDBAppendTimestamp(DuckDBAppender appender, DuckDBTimestampStruct val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_interval")]
        public static extern DuckDBState DuckDBAppendInterval(DuckDBAppender appender, DuckDBInterval val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_varchar")]
        public static extern DuckDBState DuckDBAppendVarchar(DuckDBAppender appender, SafeUnmanagedMemoryHandle val);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_varchar_length")]
        public static extern DuckDBState DuckDBAppendVarchar(DuckDBAppender appender, SafeUnmanagedMemoryHandle val, int length);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_blob")]
        public static extern unsafe DuckDBState DuckDBAppendBlob(DuckDBAppender appender, byte* data, int length);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_null")]
        public static extern DuckDBState DuckDBAppendNull(DuckDBAppender appender);

        [SuppressGCTransition]
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_data_chunk")]
        public static extern DuckDBState DuckDBAppendDataChunk(DuckDBAppender appender, DuckDBDataChunk chunk);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_default_to_chunk")]
        public static extern DuckDBState DuckDBAppendDefaultToChunk(DuckDBAppender appender, DuckDBDataChunk chunk, int column, ulong row);
    }
}