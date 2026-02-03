using System;
using System.Linq;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

public partial class NativeMethods
{
    public static class Value
    {
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_value")]
        public static extern void DuckDBDestroyValue(ref IntPtr config);
        
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_varchar")]
        public static extern DuckDBValue DuckDBCreateVarchar(SafeUnmanagedMemoryHandle value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_bool")]
        public static extern DuckDBValue DuckDBCreateBool(bool value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_int8")]
        public static extern DuckDBValue DuckDBCreateInt8(sbyte value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_uint8")]
        public static extern DuckDBValue DuckDBCreateUInt8(byte value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_int16")]
        public static extern DuckDBValue DuckDBCreateInt16(short value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_uint16")]
        public static extern DuckDBValue DuckDBCreateUInt16(ushort value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_int32")]
        public static extern DuckDBValue DuckDBCreateInt32(int value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_uint32")]
        public static extern DuckDBValue DuckDBCreateUInt32(uint value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_int64")]
        public static extern DuckDBValue DuckDBCreateInt64(long value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_uint64")]
        public static extern DuckDBValue DuckDBCreateUInt64(ulong value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_hugeint")]
        public static extern DuckDBValue DuckDBCreateHugeInt(DuckDBHugeInt value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_uhugeint")]
        public static extern DuckDBValue DuckDBCreateUHugeInt(DuckDBUHugeInt value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_decimal")]
        public static extern DuckDBValue DuckDBCreateDecimal(DuckDBDecimal value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_float")]
        public static extern DuckDBValue DuckDBCreateFloat(float value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_double")]
        public static extern DuckDBValue DuckDBCreateDouble(double value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_date")]
        public static extern DuckDBValue DuckDBCreateDate(DuckDBDate value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_time")]
        public static extern DuckDBValue DuckDBCreateTime(DuckDBTime value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_time_tz_value")]
        public static extern DuckDBValue DuckDBCreateTimeTz(DuckDBTimeTzStruct value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_timestamp")]
        public static extern DuckDBValue DuckDBCreateTimestamp(DuckDBTimestampStruct value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_timestamp_tz")]
        public static extern DuckDBValue DuckDBCreateTimestampTz(DuckDBTimestampStruct value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_timestamp_s")]
        public static extern DuckDBValue DuckDBCreateTimestampS(DuckDBTimestampStruct value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_timestamp_ms")]
        public static extern DuckDBValue DuckDBCreateTimestampMs(DuckDBTimestampStruct value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_timestamp_ns")]
        public static extern DuckDBValue DuckDBCreateTimestampNs(DuckDBTimestampStruct value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_interval")]
        public static extern DuckDBValue DuckDBCreateInterval(DuckDBInterval value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_blob")]
        public static extern DuckDBValue DuckDBCreateBlob([In] byte[] value, long length);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_uuid")]
        public static extern DuckDBValue DuckDBCreateUuid(DuckDBHugeInt value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_bool")]
        public static extern bool DuckDBGetBool(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_int8")]
        public static extern sbyte DuckDBGetInt8(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_uint8")]
        public static extern byte DuckDBGetUInt8(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_int16")]
        public static extern short DuckDBGetInt16(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_uint16")]
        public static extern ushort DuckDBGetUInt16(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_int32")]
        public static extern int DuckDBGetInt32(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_uint32")]
        public static extern uint DuckDBGetUInt32(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_int64")]
        public static extern long DuckDBGetInt64(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_uint64")]
        public static extern ulong DuckDBGetUInt64(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_hugeint")]
        public static extern DuckDBHugeInt DuckDBGetHugeInt(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_uhugeint")]
        public static extern DuckDBUHugeInt DuckDBGetUHugeInt(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_float")]
        public static extern float DuckDBGetFloat(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_double")]
        public static extern double DuckDBGetDouble(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_date")]
        public static extern DuckDBDate DuckDBGetDate(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_time")]
        public static extern DuckDBTime DuckDBGetTime(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_time_tz")] 
        public static extern DuckDBTimeTzStruct DuckDBGetTimeTz(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_timestamp")]
        public static extern DuckDBTimestampStruct DuckDBGetTimestamp(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_timestamp_s")]
        public static extern DuckDBTimestampStruct DuckDBGetTimestampS(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_timestamp_ms")]
        public static extern DuckDBTimestampStruct DuckDBGetTimestampMs(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_timestamp_ns")]
        public static extern DuckDBTimestampStruct DuckDBGetTimestampNs(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_interval")]
        public static extern DuckDBInterval DuckDBGetInterval(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention =  CallingConvention.Cdecl, EntryPoint = "duckdb_get_value_type")]
        public static extern DuckDBLogicalType DuckDBGetValueType(DuckDBValue value);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_get_varchar")]
        public static extern string DuckDBGetVarchar(DuckDBValue value);
        
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_list_value")]
        public static extern DuckDBValue DuckDBCreateListValue(DuckDBLogicalType logicalType, IntPtr[] values, long count);
        
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_array_value")]
        public static extern DuckDBValue DuckDBCreateArrayValue(DuckDBLogicalType logicalType, IntPtr[] values, long count);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_create_null_value")]
        public static extern DuckDBValue DuckDBCreateNullValue();

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_is_null_value")]
        public static extern bool DuckDBIsNullValue(DuckDBValue value);

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