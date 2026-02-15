namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#safe-fetch-functions
    public static partial class Types
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_boolean")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalAs(UnmanagedType.I1)]
        public static partial bool DuckDBValueBoolean(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_int8")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial sbyte DuckDBValueInt8(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_int16")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial short DuckDBValueInt16(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_int32")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial int DuckDBValueInt32(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_int64")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial long DuckDBValueInt64(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_decimal")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBDecimal DuckDBValueDecimal(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_uint8")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial byte DuckDBValueUInt8(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_uint16")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial ushort DuckDBValueUInt16(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_uint32")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial uint DuckDBValueUInt32(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_uint64")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial ulong DuckDBValueUInt64(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_float")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial float DuckDBValueFloat(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_double")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial double DuckDBValueDouble(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_interval")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBInterval DuckDBValueInterval(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_varchar")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial IntPtr DuckDBValueVarchar(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_blob")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBBlob DuckDBValueBlob(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_date")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBDate DuckDBValueDate(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_time")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBTime DuckDBValueTime(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_value_timestamp")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBTimestampStruct DuckDBValueTimestamp(ref DuckDBResult result, long col, long row);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_result_get_chunk")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBDataChunk DuckDBResultGetChunk(DuckDBResult result, long chunkIndex);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_result_is_streaming")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial byte DuckDBResultIsStreaming(DuckDBResult result);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_result_chunk_count")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial long DuckDBResultChunkCount(DuckDBResult result);
    }
}
