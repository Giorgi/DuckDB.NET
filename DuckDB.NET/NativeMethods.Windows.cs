using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET
{
    public class NativeMethods
    {
        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_open")]
        public static extern DuckDBState DuckDBOpen(string path, out IntPtr database);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_close")]
        public static extern void DuckDBClose(out IntPtr database);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_connect")]
        public static extern DuckDBState DuckDBConnect(IntPtr database, out IntPtr connection);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_disconnect")]
        public static extern void DuckDBDisconnect(out IntPtr connection);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_query")]
        public static extern DuckDBState DuckDBQuery(IntPtr connection, string query, out DuckDBResult result);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_result")]
        public static extern void DuckDBDestroyResult(out DuckDBResult result);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_column_name")]
        public static extern string DuckDBColumnName(DuckDBResult result, long col);


        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_boolean")]
        public static extern bool DuckDBValueBoolean(DuckDBResult result, long col, long row);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int8")]
        public static extern sbyte DuckDBValueInt8(DuckDBResult result, long col, long row);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int16")]
        public static extern short DuckDBValueInt16(DuckDBResult result, long col, long row);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int32")]
        public static extern int DuckDBValueInt32(DuckDBResult result, long col, long row);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int64")]
        public static extern long DuckDBValueInt64(DuckDBResult result, long col, long row);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_float")]
        public static extern float DuckDBValueFloat(DuckDBResult result, long col, long row);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_double")]
        public static extern double DuckDBValueDouble(DuckDBResult result, long col, long row);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_varchar")]
        public static extern string DuckDBValueVarchar(DuckDBResult result, long col, long row);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_prepare")]
        public static extern DuckDBState DuckDBPrepare(IntPtr connection, string query, out IntPtr preparedStatement);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_prepare")]
        public static extern DuckDBState DuckDBParams(IntPtr preparedStatement, out long numberOfParams);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_boolean")]
        public static extern DuckDBState DuckDBBindBoolean(IntPtr preparedStatement, long index, bool val);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_int8")]
        public static extern DuckDBState DuckDBBindInt8(IntPtr preparedStatement, long index, sbyte val);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_int16")]
        public static extern DuckDBState DuckDBBindInt16(IntPtr preparedStatement, long index, short val);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_int32")]
        public static extern DuckDBState DuckDBBindInt32(IntPtr preparedStatement, long index, int val);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_int64")]
        public static extern DuckDBState DuckDBBindInt64(IntPtr preparedStatement, long index, long val);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_float")]
        public static extern DuckDBState DuckDBBindFloat(IntPtr preparedStatement, long index, float val);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_double")]
        public static extern DuckDBState DuckDBBindDouble(IntPtr preparedStatement, long index, double val);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_varchar")]
        public static extern DuckDBState DuckDBBindVarchar(IntPtr preparedStatement, long index, string val);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_null")]
        public static extern DuckDBState DuckDBBindNull(IntPtr preparedStatement, long index);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_execute_prepared")]
        public static extern DuckDBState DuckDBExecutePrepared(IntPtr preparedStatement, out DuckDBResult result);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_prepare")]
        public static extern void DuckDBDestroyPrepare(out IntPtr preparedStatement);
    }
}
