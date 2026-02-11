using System.Linq;

namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static partial class Value
    {
        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_destroy_value")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial void DuckDBDestroyValue(ref IntPtr config);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_varchar")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateVarchar(SafeUnmanagedMemoryHandle value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_bool")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateBool([MarshalAs(UnmanagedType.I1)] bool value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_int8")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateInt8(sbyte value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_uint8")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateUInt8(byte value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_int16")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateInt16(short value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_uint16")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateUInt16(ushort value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_int32")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateInt32(int value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_uint32")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateUInt32(uint value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_int64")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateInt64(long value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_uint64")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateUInt64(ulong value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_hugeint")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateHugeInt(DuckDBHugeInt value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_uhugeint")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateUHugeInt(DuckDBUHugeInt value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_decimal")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateDecimal(DuckDBDecimal value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_float")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateFloat(float value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_double")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateDouble(double value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_date")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateDate(DuckDBDate value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_time")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateTime(DuckDBTime value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_time_tz_value")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateTimeTz(DuckDBTimeTzStruct value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_timestamp")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateTimestamp(DuckDBTimestampStruct value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_timestamp_tz")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateTimestampTz(DuckDBTimestampStruct value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_timestamp_s")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateTimestampS(DuckDBTimestampStruct value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_timestamp_ms")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateTimestampMs(DuckDBTimestampStruct value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_timestamp_ns")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateTimestampNs(DuckDBTimestampStruct value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_interval")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateInterval(DuckDBInterval value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_blob")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateBlob([In] byte[] value, long length);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_uuid")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateUuid(DuckDBHugeInt value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_bool")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalAs(UnmanagedType.I1)]
        public static partial bool DuckDBGetBool(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_int8")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial sbyte DuckDBGetInt8(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_uint8")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial byte DuckDBGetUInt8(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_int16")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial short DuckDBGetInt16(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_uint16")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial ushort DuckDBGetUInt16(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_int32")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial int DuckDBGetInt32(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_uint32")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial uint DuckDBGetUInt32(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_int64")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial long DuckDBGetInt64(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_uint64")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial ulong DuckDBGetUInt64(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_hugeint")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBHugeInt DuckDBGetHugeInt(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_uhugeint")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBUHugeInt DuckDBGetUHugeInt(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_float")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial float DuckDBGetFloat(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_double")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial double DuckDBGetDouble(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_date")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBDate DuckDBGetDate(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_time")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBTime DuckDBGetTime(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_time_tz")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBTimeTzStruct DuckDBGetTimeTz(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_timestamp")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBTimestampStruct DuckDBGetTimestamp(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_timestamp_s")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBTimestampStruct DuckDBGetTimestampS(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_timestamp_ms")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBTimestampStruct DuckDBGetTimestampMs(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_timestamp_ns")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBTimestampStruct DuckDBGetTimestampNs(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_interval")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBInterval DuckDBGetInterval(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_value_type")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBLogicalType DuckDBGetValueType(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_get_varchar", StringMarshalling = StringMarshalling.Utf8)]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial string DuckDBGetVarchar(DuckDBValue value);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_list_value")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateListValue(DuckDBLogicalType logicalType, IntPtr[] values, long count);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_array_value")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateArrayValue(DuckDBLogicalType logicalType, IntPtr[] values, long count);

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_create_null_value")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        public static partial DuckDBValue DuckDBCreateNullValue();

        [LibraryImport(DuckDbLibrary, EntryPoint = "duckdb_is_null_value")]
        [UnmanagedCallConv(CallConvs = [typeof(CallConvCdecl)])]
        [return: MarshalAs(UnmanagedType.I1)]
        public static partial bool DuckDBIsNullValue(DuckDBValue value);

        public static DuckDBValue DuckDBCreateListValue(DuckDBLogicalType logicalType, DuckDBValue[] values, int count)
        {
            var duckDBValue = DuckDBCreateListValue(logicalType, values.Select(item => item.DangerousGetHandle()).ToArray(), count);

            duckDBValue.SetChildValues(values);

            return duckDBValue;
        }

        public static DuckDBValue DuckDBCreateArrayValue(DuckDBLogicalType logicalType, DuckDBValue[] values, int count)
        {
            var duckDBValue = DuckDBCreateArrayValue(logicalType, values.Select(item => item.DangerousGetHandle()).ToArray(), count);

            duckDBValue.SetChildValues(values);

            return duckDBValue;
        }
    }
}
