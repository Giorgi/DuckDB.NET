using System;
using System.Runtime.InteropServices;

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

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_end_row")]
        public static extern DuckDBState DuckDBAppenderEndRow(DuckDBAppender appender);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_close")]
        public static extern DuckDBState DuckDBAppenderClose(DuckDBAppender appender);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_appender_destroy")]
        public static extern DuckDBState DuckDBDestroyAppender(out IntPtr appender);

#if NET5_0_OR_GREATER
        [SuppressGCTransition]
#endif
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_bool")]
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
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_hugeint")]
        public static extern DuckDBState DuckDBAppendHugeInt(DuckDBAppender appender, DuckDBHugeInt val);

#if NET5_0_OR_GREATER
        [SuppressGCTransition]
#endif
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_uhugeint")]
        public static extern DuckDBState DuckDBAppendUHugeInt(DuckDBAppender appender, DuckDBUHugeInt val);

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
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_timestamp")]
        public static extern DuckDBState DuckDBAppendTimestamp(DuckDBAppender appender, DuckDBTimestampStruct val);

#if NET5_0_OR_GREATER
        [SuppressGCTransition]
#endif
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_interval")]
        public static extern DuckDBState DuckDBAppendInterval(DuckDBAppender appender, DuckDBInterval val);

#if NET5_0_OR_GREATER
        [SuppressGCTransition]
#endif
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_varchar")]
        public static extern DuckDBState DuckDBAppendVarchar(DuckDBAppender appender, SafeUnmanagedMemoryHandle val);

#if NET5_0_OR_GREATER
        [SuppressGCTransition]
#endif
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_varchar_length")]
        public static extern DuckDBState DuckDBAppendVarchar(DuckDBAppender appender, SafeUnmanagedMemoryHandle val, int length);

#if NET5_0_OR_GREATER
        [SuppressGCTransition]
#endif
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_blob")]
        public static extern unsafe DuckDBState DuckDBAppendBlob(DuckDBAppender appender, byte* data, int length);

#if NET5_0_OR_GREATER
        [SuppressGCTransition]
#endif
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_null")]
        public static extern DuckDBState DuckDBAppendNull(DuckDBAppender appender);

#if NET5_0_OR_GREATER
        [SuppressGCTransition]
#endif
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_data_chunk")]
        public static extern DuckDBState DuckDBAppendDataChunk(DuckDBAppender appender, DuckDBDataChunk chunk);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_append_default_to_chunk")]
        public static extern DuckDBState DuckDBAppendDefaultToChunk(DuckDBAppender appender, DuckDBDataChunk chunk, ulong row, int column);
    }
}