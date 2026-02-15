namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    //https://duckdb.org/docs/api/c/api#prepared-statements
    //https://duckdb.org/docs/api/c/api#bind-values-to-prepared-statements
    public static partial class PreparedStatements
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_prepare", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBPrepare(DuckDBNativeConnection connection, string query, out DuckDBPreparedStatement preparedStatement);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_prepare")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyPrepare(ref IntPtr preparedStatement);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_prepare_error")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalUsing(typeof(DuckDBOwnedStringMarshaller))]
        public static partial string DuckDBPrepareError(DuckDBPreparedStatement preparedStatement);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_nparams")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial long DuckDBParams(DuckDBPreparedStatement preparedStatement);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_value")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindValue(DuckDBPreparedStatement preparedStatement, long index, DuckDBValue val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_parameter_index", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindParameterIndex(DuckDBPreparedStatement preparedStatement, out int index, string name);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_boolean")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindBoolean(DuckDBPreparedStatement preparedStatement, long index, [MarshalAs(UnmanagedType.I1)] bool val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_int8")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindInt8(DuckDBPreparedStatement preparedStatement, long index, sbyte val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_int16")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindInt16(DuckDBPreparedStatement preparedStatement, long index, short val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_int32")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindInt32(DuckDBPreparedStatement preparedStatement, long index, int val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_int64")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindInt64(DuckDBPreparedStatement preparedStatement, long index, long val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_hugeint")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindHugeInt(DuckDBPreparedStatement preparedStatement, long index, DuckDBHugeInt val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_uint8")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindUInt8(DuckDBPreparedStatement preparedStatement, long index, byte val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_uint16")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindUInt16(DuckDBPreparedStatement preparedStatement, long index, ushort val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_uint32")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindUInt32(DuckDBPreparedStatement preparedStatement, long index, uint val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_uint64")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindUInt64(DuckDBPreparedStatement preparedStatement, long index, ulong val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_float")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindFloat(DuckDBPreparedStatement preparedStatement, long index, float val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_double")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindDouble(DuckDBPreparedStatement preparedStatement, long index, double val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_date")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindDate(DuckDBPreparedStatement preparedStatement, long index, DuckDBDate val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_time")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindTime(DuckDBPreparedStatement preparedStatement, long index, DuckDBTime val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_timestamp")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindTimestamp(DuckDBPreparedStatement preparedStatement, long index, DuckDBTimestampStruct val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_varchar", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindVarchar(DuckDBPreparedStatement preparedStatement, long index, string val);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_blob")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindBlob(DuckDBPreparedStatement preparedStatement, long index, [In] byte[] data, long length);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_bind_null")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBBindNull(DuckDBPreparedStatement preparedStatement, long index);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_execute_prepared")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBExecutePrepared(DuckDBPreparedStatement preparedStatement, out DuckDBResult result);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_execute_prepared_streaming")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBState DuckDBExecutePreparedStreaming(DuckDBPreparedStatement preparedStatement, out DuckDBResult result);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_param_logical_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBLogicalType DuckDBParamLogicalType(DuckDBPreparedStatement preparedStatement, long index);
    }
}
