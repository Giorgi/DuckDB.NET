using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET.MacOS
{
    class MacOSBindNativeMethods : IBindNativeMethods
    {
        public DuckDBState DuckDBOpen(string path, out DuckDBDatabase database)
        {
            return NativeMethods.DuckDBOpen(path, out database);
        }

        public DuckDBState DuckDBOpen(string path, out DuckDBDatabase database, out IntPtr error)
        {
            return NativeMethods.DuckDBOpen(path, out database, IntPtr.Zero, out error);
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

        public DuckDBState DuckDBQuery(DuckDBNativeConnection connection, SafeUnmanagedMemoryHandle query, DuckDBResult result)
        {
            return NativeMethods.DuckDBQuery(connection, query, result);
        }

        public void DuckDBDestroyResult(DuckDBResult result)
        {
            NativeMethods.DuckDBDestroyResult(result);
        }

        public string DuckDBColumnName(DuckDBResult result, long col)
        {
            return NativeMethods.DuckDBColumnName(result, col).ToManagedString(false);
        }

        public DuckDBType DuckDBColumnType(DuckDBResult result, long col)
        {
            return NativeMethods.DuckDBColumnType(result, col);
        }

        public long DuckDBColumnCount(DuckDBResult result)
        {
            return NativeMethods.DuckDBColumnCount(result);
        }

        public long DuckDBRowCount(DuckDBResult result)
        {
            return NativeMethods.DuckDBRowCount(result);
        }

        public long DuckDBRowsChanged(DuckDBResult result)
        {
            return NativeMethods.DuckDBRowsChanged(result);
        }

        public IntPtr DuckDBColumnData(DuckDBResult result, long col)
        {
            return NativeMethods.DuckDBColumnData(result, col);
        }

        public IntPtr DuckDBNullmaskData(DuckDBResult result, long col)
        {
            return NativeMethods.DuckDBNullmaskData(result, col);
        }

        public string DuckDBResultError(DuckDBResult result)
        {
            return NativeMethods.DuckDBResultError(result).ToManagedString(false);
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

        public IntPtr DuckDBValueVarchar(DuckDBResult result, long col, long row)
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

        public DuckDBState DuckDBExecutePrepared(DuckDBPreparedStatement preparedStatement, DuckDBResult result)
        {
            return NativeMethods.DuckDBExecutePrepared(preparedStatement, result);
        }

        public void DuckDBDestroyPrepare(out IntPtr preparedStatement)
        {
            NativeMethods.DuckDBDestroyPrepare(out preparedStatement);
        }

        /// <inheritdoc />
        public void DuckDBFree(IntPtr ptr)
        {
            NativeMethods.DuckDBFree(ptr);
        }
    }

    public class NativeMethods
    {
        private const string DuckDbLibrary = "libduckdb.dylib";

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_open")]
        public static extern DuckDBState DuckDBOpen(string path, out DuckDBDatabase database);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_open_ext")]
        public static extern DuckDBState DuckDBOpen(string path, out DuckDBDatabase database, IntPtr config, out IntPtr error);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_close")]
        public static extern void DuckDBClose(out IntPtr database);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_connect")]
        public static extern DuckDBState DuckDBConnect(DuckDBDatabase database, out DuckDBNativeConnection connection);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_disconnect")]
        public static extern void DuckDBDisconnect(out IntPtr connection);
        
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_query")]
        public static extern DuckDBState DuckDBQuery(DuckDBNativeConnection connection, string query, [In, Out] DuckDBResult result);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_query")]
        public static extern DuckDBState DuckDBQuery(DuckDBNativeConnection connection, SafeUnmanagedMemoryHandle query, [In, Out] DuckDBResult result);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_result")]
        public static extern void DuckDBDestroyResult([In, Out] DuckDBResult result);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_column_name")]
        public static extern IntPtr DuckDBColumnName([In, Out] DuckDBResult result, long col);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_column_type")]
        public static extern DuckDBType DuckDBColumnType(DuckDBResult result, long col);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_column_count")]
        public static extern long DuckDBColumnCount(DuckDBResult result);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_row_count")]
        public static extern long DuckDBRowCount(DuckDBResult result);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_rows_changed")]
        public static extern long DuckDBRowsChanged(DuckDBResult result);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_column_data")]
        public static extern IntPtr DuckDBColumnData(DuckDBResult result, long col);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_nullmask_data")]
        public static extern IntPtr DuckDBNullmaskData(DuckDBResult result, long col);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_result_error")]
        public static extern IntPtr DuckDBResultError(DuckDBResult result);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_boolean")]
        public static extern bool DuckDBValueBoolean([In, Out] DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int8")]
        public static extern sbyte DuckDBValueInt8([In, Out] DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int16")]
        public static extern short DuckDBValueInt16([In, Out] DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int32")]
        public static extern int DuckDBValueInt32([In, Out] DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_int64")]
        public static extern long DuckDBValueInt64([In, Out] DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_float")]
        public static extern float DuckDBValueFloat([In, Out] DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_double")]
        public static extern double DuckDBValueDouble([In, Out] DuckDBResult result, long col, long row);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_value_varchar")]
        public static extern IntPtr DuckDBValueVarchar([In, Out] DuckDBResult result, long col, long row);
        
        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_prepare")]
        public static extern DuckDBState DuckDBPrepare(DuckDBNativeConnection connection, string query, out DuckDBPreparedStatement preparedStatement);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_nparams")]
        public static extern DuckDBState DuckDBParams(DuckDBPreparedStatement preparedStatement, out long numberOfParams);

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

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_float")]
        public static extern DuckDBState DuckDBBindFloat(DuckDBPreparedStatement preparedStatement, long index, float val);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_double")]
        public static extern DuckDBState DuckDBBindDouble(DuckDBPreparedStatement preparedStatement, long index, double val);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_varchar")]
        public static extern DuckDBState DuckDBBindVarchar(DuckDBPreparedStatement preparedStatement, long index, string val);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_bind_null")]
        public static extern DuckDBState DuckDBBindNull(DuckDBPreparedStatement preparedStatement, long index);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_execute_prepared")]
        public static extern DuckDBState DuckDBExecutePrepared(DuckDBPreparedStatement preparedStatement, [In, Out] DuckDBResult result);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_destroy_prepare")]
        public static extern void DuckDBDestroyPrepare(out IntPtr preparedStatement);

        [DllImport(DuckDbLibrary, CallingConvention = CallingConvention.Cdecl, EntryPoint = "duckdb_free")]
        public static extern void DuckDBFree(IntPtr ptr);
    }
}
