using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Linux
{
    class LinuxBindNativeMethods : IBindNativeMethods
    {
        public DuckDBState DuckDBOpen(string path, out DuckDBDatabase database)
        {
            return NativeMethods.DuckDBOpen(path, out database);
        }

        public void DuckDBClose(out IntPtr database)
        {
            NativeMethods.DuckDBClose(out database);
        }

        public DuckDBState DuckDBConnect(DuckDBDatabase database, out DuckDBNativeConnection connection)
        {
            return NativeMethods.DuckDBConnect(database, out connection);
        }

        public void DuckDBDisconnect(out IntPtr connection)
        {
            NativeMethods.DuckDBDisconnect(out connection);
        }

        public DuckDBState DuckDBQuery(DuckDBNativeConnection connection, string query, out DuckDBResult result)
        {
            return NativeMethods.DuckDBQuery(connection, query, out result);
        }

        public void DuckDBDestroyResult(out DuckDBResult result)
        {
            NativeMethods.DuckDBDestroyResult(out result);
        }

        public string DuckDBColumnName(DuckDBResult result, long col)
        {
            return NativeMethods.DuckDBColumnName(result, col);
        }

        public bool DuckDBValueBoolean(DuckDBResult result, long col, long row)
        {
            return NativeMethods.DuckDBValueBoolean(result, col, row);
        }

        public sbyte DuckDBValueInt8(DuckDBResult result, long col, long row)
        {
            return NativeMethods.DuckDBValueInt8(result, col, row);
        }

        public short DuckDBValueInt16(DuckDBResult result, long col, long row)
        {
            return NativeMethods.DuckDBValueInt16(result, col, row);
        }

        public int DuckDBValueInt32(DuckDBResult result, long col, long row)
        {
            return NativeMethods.DuckDBValueInt32(result, col, row);
        }

        public long DuckDBValueInt64(DuckDBResult result, long col, long row)
        {
            return NativeMethods.DuckDBValueInt64(result, col, row);
        }

        public float DuckDBValueFloat(DuckDBResult result, long col, long row)
        {
            return NativeMethods.DuckDBValueFloat(result, col, row);
        }

        public double DuckDBValueDouble(DuckDBResult result, long col, long row)
        {
            return NativeMethods.DuckDBValueDouble(result, col, row);
        }

        public string DuckDBValueVarchar(DuckDBResult result, long col, long row)
        {
            return NativeMethods.DuckDBValueVarchar(result, col, row);
        }

        public DuckDBState DuckDBPrepare(DuckDBNativeConnection connection, string query, out DuckDBPreparedStatement preparedStatement)
        {
            return NativeMethods.DuckDBPrepare(connection, query, out preparedStatement);
        }

        public DuckDBState DuckDBParams(DuckDBPreparedStatement preparedStatement, out long numberOfParams)
        {
            return NativeMethods.DuckDBParams(preparedStatement, out numberOfParams);
        }

        public DuckDBState DuckDBBindBoolean(DuckDBPreparedStatement preparedStatement, long index, bool val)
        {
            return NativeMethods.DuckDBBindBoolean(preparedStatement, index, val);
        }

        public DuckDBState DuckDBBindInt8(DuckDBPreparedStatement preparedStatement, long index, sbyte val)
        {
            return NativeMethods.DuckDBBindInt8(preparedStatement, index, val);
        }

        public DuckDBState DuckDBBindInt16(DuckDBPreparedStatement preparedStatement, long index, short val)
        {
            return NativeMethods.DuckDBBindInt16(preparedStatement, index, val);
        }

        public DuckDBState DuckDBBindInt32(DuckDBPreparedStatement preparedStatement, long index, int val)
        {
            return NativeMethods.DuckDBBindInt32(preparedStatement, index, val);
        }

        public DuckDBState DuckDBBindInt64(DuckDBPreparedStatement preparedStatement, long index, long val)
        {
            return NativeMethods.DuckDBBindInt64(preparedStatement, index, val);
        }

        public DuckDBState DuckDBBindFloat(DuckDBPreparedStatement preparedStatement, long index, float val)
        {
            return NativeMethods.DuckDBBindFloat(preparedStatement, index, val);
        }

        public DuckDBState DuckDBBindDouble(DuckDBPreparedStatement preparedStatement, long index, double val)
        {
            return NativeMethods.DuckDBBindDouble(preparedStatement, index, val);
        }

        public DuckDBState DuckDBBindVarchar(DuckDBPreparedStatement preparedStatement, long index, string val)
        {
            return NativeMethods.DuckDBBindVarchar(preparedStatement, index, val);
        }

        public DuckDBState DuckDBBindNull(DuckDBPreparedStatement preparedStatement, long index)
        {
            return NativeMethods.DuckDBBindNull(preparedStatement, index);
        }

        public DuckDBState DuckDBExecutePrepared(DuckDBPreparedStatement preparedStatement, out DuckDBResult result)
        {
            return NativeMethods.DuckDBExecutePrepared(preparedStatement, out result);
        }

        public void DuckDBDestroyPrepare(out IntPtr preparedStatement)
        {
            NativeMethods.DuckDBDestroyPrepare(out preparedStatement);
        }
    }

    public class NativeMethods
    {
        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_open")]
        public static extern DuckDBState DuckDBOpen(string path, out DuckDBDatabase database);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_close")]
        public static extern void DuckDBClose(out IntPtr database);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_connect")]
        public static extern DuckDBState DuckDBConnect(DuckDBDatabase database, out DuckDBNativeConnection connection);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_disconnect")]
        public static extern void DuckDBDisconnect(out IntPtr connection);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_query")]
        public static extern DuckDBState DuckDBQuery(DuckDBNativeConnection connection, string query, out DuckDBResult result);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_result")]
        public static extern void DuckDBDestroyResult(out DuckDBResult result);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_column_name")]
        public static extern string DuckDBColumnName(DuckDBResult result, long col);


        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_boolean")]
        public static extern bool DuckDBValueBoolean(DuckDBResult result, long col, long row);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int8")]
        public static extern sbyte DuckDBValueInt8(DuckDBResult result, long col, long row);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int16")]
        public static extern short DuckDBValueInt16(DuckDBResult result, long col, long row);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int32")]
        public static extern int DuckDBValueInt32(DuckDBResult result, long col, long row);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int64")]
        public static extern long DuckDBValueInt64(DuckDBResult result, long col, long row);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_float")]
        public static extern float DuckDBValueFloat(DuckDBResult result, long col, long row);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_double")]
        public static extern double DuckDBValueDouble(DuckDBResult result, long col, long row);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_varchar")]
        public static extern string DuckDBValueVarchar(DuckDBResult result, long col, long row);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_prepare")]
        public static extern DuckDBState DuckDBPrepare(DuckDBNativeConnection connection, string query, out DuckDBPreparedStatement preparedStatement);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_nparams")]
        public static extern DuckDBState DuckDBParams(DuckDBPreparedStatement preparedStatement, out long numberOfParams);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_boolean")]
        public static extern DuckDBState DuckDBBindBoolean(DuckDBPreparedStatement preparedStatement, long index, bool val);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_int8")]
        public static extern DuckDBState DuckDBBindInt8(DuckDBPreparedStatement preparedStatement, long index, sbyte val);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_int16")]
        public static extern DuckDBState DuckDBBindInt16(DuckDBPreparedStatement preparedStatement, long index, short val);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_int32")]
        public static extern DuckDBState DuckDBBindInt32(DuckDBPreparedStatement preparedStatement, long index, int val);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_int64")]
        public static extern DuckDBState DuckDBBindInt64(DuckDBPreparedStatement preparedStatement, long index, long val);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_float")]
        public static extern DuckDBState DuckDBBindFloat(DuckDBPreparedStatement preparedStatement, long index, float val);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_double")]
        public static extern DuckDBState DuckDBBindDouble(DuckDBPreparedStatement preparedStatement, long index, double val);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_varchar")]
        public static extern DuckDBState DuckDBBindVarchar(DuckDBPreparedStatement preparedStatement, long index, string val);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_null")]
        public static extern DuckDBState DuckDBBindNull(DuckDBPreparedStatement preparedStatement, long index);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_execute_prepared")]
        public static extern DuckDBState DuckDBExecutePrepared(DuckDBPreparedStatement preparedStatement, out DuckDBResult result);

        [DllImport("libduckdb.so", CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_prepare")]
        public static extern void DuckDBDestroyPrepare(out IntPtr preparedStatement);
    }
}
