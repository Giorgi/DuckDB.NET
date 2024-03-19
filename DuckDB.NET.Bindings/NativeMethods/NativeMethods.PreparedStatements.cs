using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

public partial class NativeMethods
{
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

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_parameter_index")]
        public static extern DuckDBState DuckDBBindParameterIndex(DuckDBPreparedStatement preparedStatement, out int index, string name);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_parameter_index")]
        public static extern DuckDBState DuckDBBindParameterIndex(DuckDBPreparedStatement preparedStatement, out int index, SafeUnmanagedMemoryHandle name);

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
        public static extern DuckDBState DuckDBExecutePrepared(DuckDBPreparedStatement preparedStatement, out DuckDBResult result);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_execute_prepared_streaming")]
        public static extern DuckDBState DuckDBExecutePreparedStreaming(DuckDBPreparedStatement preparedStatement, out DuckDBResult result);
    }
}