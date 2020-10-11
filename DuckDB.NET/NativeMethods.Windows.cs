using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace DuckDB.NET
{
    public class NativeMethods
    {
        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_open")]
        public static extern DuckdbState DuckDbOpen(string path, out IntPtr database);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_close")]
        public static extern void DuckdbClose(out IntPtr database);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_connect")]
        public static extern DuckdbState DuckdbConnect(IntPtr database, out IntPtr connection);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_disconnect")]
        public static extern void DuckdbDisconnect(out IntPtr connection);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_query")]
        public static extern DuckdbState DuckdbQuery(IntPtr connection, string query, out DuckdbResult result);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_result")]
        public static extern void DuckdbDestroyResult(out DuckdbResult result);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_column_name")]
        public static extern string DuckdbColumnName(IntPtr result, long col);


        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_boolean")]
        public static extern bool DuckdbValueBoolean(DuckdbResult result, long col, long row);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int8")]
        public static extern sbyte DuckdbValueInt8(DuckdbResult result, long col, long row);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int16")]
        public static extern short DuckdbValueInt16(DuckdbResult result, long col, long row);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int32")]
        public static extern int DuckdbValueInt32(DuckdbResult result, long col, long row);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int64")]
        public static extern long DuckdbValueInt64(DuckdbResult result, long col, long row);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_float")]
        public static extern float DuckdbValueFloat(DuckdbResult result, long col, long row);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_double")]
        public static extern double DuckdbValueDouble(DuckdbResult result, long col, long row);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_varchar")]
        public static extern string DuckdbValueVarchar(DuckdbResult result, long col, long row);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_prepare")]
        public static extern DuckdbState DuckdbPrepare(IntPtr connection, string query, out IntPtr preparedStatement);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_prepare")]
        public static extern DuckdbState DuckdbParams(IntPtr preparedStatement, out long numberOfParams);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_boolean")]
        public static extern  DuckdbState DuckdbBindBoolean(IntPtr preparedStatement, long index, bool val);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_int8")]
        public static extern  DuckdbState DuckdbBindInt8(IntPtr preparedStatement, long index, sbyte val);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_int16")]
        public static extern  DuckdbState DuckdbBindInt16(IntPtr preparedStatement, long index, short val);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_int32")]
        public static extern  DuckdbState DuckdbBindInt32(IntPtr preparedStatement, long index, int val);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_int64")]
        public static extern  DuckdbState DuckdbBindInt64(IntPtr preparedStatement, long index, long val);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_float")]
        public static extern  DuckdbState DuckdbBindFloat(IntPtr preparedStatement, long index, float val);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_double")]
        public static extern  DuckdbState DuckdbBindDouble(IntPtr preparedStatement, long index, double val);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_varchar")]
        public static extern  DuckdbState DuckdbBindVarchar(IntPtr preparedStatement, long index, string val);
        
        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_null")]
        public static extern  DuckdbState DuckdbBindNull(IntPtr preparedStatement, long index);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_execute_prepared")]
        public static extern  DuckdbState DuckdbExecutePrepared(IntPtr preparedStatement, out DuckdbResult result);

        [DllImport("duckdb.dll", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_prepare")]
        public static extern  void DuckdbDestroyPrepare(out IntPtr preparedStatement);
    }
}
