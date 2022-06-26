using System;

namespace DuckDB.NET
{
    public interface IBindNativeMethods
    {
        //Grouped according to https://duckdb.org/docs/api/c/overview

        #region Startup

        DuckDBState DuckDBOpen(string path, out DuckDBDatabase database);

        void DuckDBClose(out IntPtr database);

        DuckDBState DuckDBConnect(DuckDBDatabase database, out DuckDBNativeConnection connection);

        void DuckDBDisconnect(out IntPtr connection);

        #endregion

        #region Query

        DuckDBState DuckDBQuery(DuckDBNativeConnection connection, SafeUnmanagedMemoryHandle query, DuckDBResult result);

        void DuckDBDestroyResult(DuckDBResult result);

        string DuckDBColumnName(DuckDBResult result, long col);

        DuckDBType DuckDBColumnType(DuckDBResult result, long col);

        long DuckDBColumnCount(DuckDBResult result);

        long DuckDBRowCount(DuckDBResult result);

        long DuckDBRowsChanged(DuckDBResult result);

        IntPtr DuckDBColumnData(DuckDBResult result, long col);

        IntPtr DuckDBNullmaskData(DuckDBResult result, long col);

        string DuckDBResultError(DuckDBResult result);

        #endregion

        #region Types

        bool DuckDBValueBoolean(DuckDBResult result, long col, long row);

        sbyte DuckDBValueInt8(DuckDBResult result, long col, long row);

        short DuckDBValueInt16(DuckDBResult result, long col, long row);

        int DuckDBValueInt32(DuckDBResult result, long col, long row);

        long DuckDBValueInt64(DuckDBResult result, long col, long row);

        float DuckDBValueFloat(DuckDBResult result, long col, long row);

        double DuckDBValueDouble(DuckDBResult result, long col, long row);

        IntPtr DuckDBValueVarchar(DuckDBResult result, long col, long row);

        #endregion

        #region Prepared Statements

        DuckDBState DuckDBPrepare(DuckDBNativeConnection connection, string query, out DuckDBPreparedStatement preparedStatement);

        DuckDBState DuckDBParams(DuckDBPreparedStatement preparedStatement, out long numberOfParams);

        DuckDBState DuckDBBindBoolean(DuckDBPreparedStatement preparedStatement, long index, bool val);

        DuckDBState DuckDBBindInt8(DuckDBPreparedStatement preparedStatement, long index, sbyte val);

        DuckDBState DuckDBBindInt16(DuckDBPreparedStatement preparedStatement, long index, short val);

        DuckDBState DuckDBBindInt32(DuckDBPreparedStatement preparedStatement, long index, int val);

        DuckDBState DuckDBBindInt64(DuckDBPreparedStatement preparedStatement, long index, long val);

        DuckDBState DuckDBBindFloat(DuckDBPreparedStatement preparedStatement, long index, float val);

        DuckDBState DuckDBBindDouble(DuckDBPreparedStatement preparedStatement, long index, double val);

        DuckDBState DuckDBBindVarchar(DuckDBPreparedStatement preparedStatement, long index, string val);

        DuckDBState DuckDBBindNull(DuckDBPreparedStatement preparedStatement, long index);

        DuckDBState DuckDBExecutePrepared(DuckDBPreparedStatement preparedStatement, DuckDBResult result);

        void DuckDBDestroyPrepare(out IntPtr preparedStatement);

        #endregion

        #region Helpers

        void DuckDBFree(IntPtr ptr);
        
        #endregion
    }
}
