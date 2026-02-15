namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/appender
    public static partial class Appender
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_appender_create", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppenderCreate(DuckDBNativeConnection connection, string? schema, string table, out DuckDBAppender appender);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_appender_create_ext", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppenderCreateExt(DuckDBNativeConnection connection, string? catalog, string? schema, string table, out DuckDBAppender appender);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_appender_column_count")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial ulong DuckDBAppenderColumnCount(DuckDBAppender appender);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_appender_column_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBLogicalType DuckDBAppenderColumnType(DuckDBAppender appender, ulong index);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_appender_error")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalUsing(typeof(DuckDBOwnedStringMarshaller))]
        public static partial string DuckDBAppenderError(DuckDBAppender appender);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_appender_flush")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppenderFlush(DuckDBAppender appender);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_appender_clear")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppenderClear(DuckDBAppender appender);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_appender_end_row")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppenderEndRow(DuckDBAppender appender);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_appender_close")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppenderClose(DuckDBAppender appender);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_appender_destroy")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBDestroyAppender(ref IntPtr appender);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_bool")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendBool(DuckDBAppender appender, [MarshalAs(UnmanagedType.I1)] bool val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_int8")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendInt8(DuckDBAppender appender, sbyte val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_int16")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendInt16(DuckDBAppender appender, short val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_int32")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendInt32(DuckDBAppender appender, int val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_int64")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendInt64(DuckDBAppender appender, long val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_hugeint")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendHugeInt(DuckDBAppender appender, DuckDBHugeInt val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_uhugeint")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendUHugeInt(DuckDBAppender appender, DuckDBUHugeInt val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_uint8")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendUInt8(DuckDBAppender appender, byte val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_uint16")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendUInt16(DuckDBAppender appender, ushort val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_uint32")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendUInt32(DuckDBAppender appender, uint val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_uint64")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendUInt64(DuckDBAppender appender, ulong val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_float")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendFloat(DuckDBAppender appender, float val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_double")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendDouble(DuckDBAppender appender, double val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_date")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendDate(DuckDBAppender appender, DuckDBDate val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_time")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendTime(DuckDBAppender appender, DuckDBTime val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_timestamp")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendTimestamp(DuckDBAppender appender, DuckDBTimestampStruct val);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_interval")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendInterval(DuckDBAppender appender, DuckDBInterval val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_varchar", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendVarchar(DuckDBAppender appender, string val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_varchar_length", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendVarchar(DuckDBAppender appender, string val, int length);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_blob")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static unsafe partial DuckDBState DuckDBAppendBlob(DuckDBAppender appender, byte* data, int length);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_null")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendNull(DuckDBAppender appender);

        [SuppressGCTransition]
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_data_chunk")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendDataChunk(DuckDBAppender appender, DuckDBDataChunk chunk);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_append_default_to_chunk")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBAppendDefaultToChunk(DuckDBAppender appender, DuckDBDataChunk chunk, int column, ulong row);
    }
}
