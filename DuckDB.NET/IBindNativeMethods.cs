using System;

namespace DuckDB.NET
{
    public interface IBindNativeMethods
    {
        DuckDBState DuckDBOpen(string path, out DuckDBDatabase database);

        void DuckDBClose(out IntPtr database);

        DuckDBState DuckDBConnect(DuckDBDatabase database, out DuckDBNativeConnection connection);

        void DuckDBDisconnect(out IntPtr connection);

        DuckDBState DuckDBQuery(DuckDBNativeConnection connection, SafeUnmanagedMemoryHandle query, out DuckDBResult result);

        void DuckDBDestroyResult(ref DuckDBResult result);

        string DuckDBColumnName(DuckDBResult result, long col);

        bool DuckDBValueBoolean(DuckDBResult result, long col, long row);

        sbyte DuckDBValueInt8(DuckDBResult result, long col, long row);

        short DuckDBValueInt16(DuckDBResult result, long col, long row);

        int DuckDBValueInt32(DuckDBResult result, long col, long row);

        long DuckDBValueInt64(DuckDBResult result, long col, long row);

        float DuckDBValueFloat(DuckDBResult result, long col, long row);

        double DuckDBValueDouble(DuckDBResult result, long col, long row);

        IntPtr DuckDBValueVarchar(DuckDBResult result, long col, long row);

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

        DuckDBState DuckDBExecutePrepared(DuckDBPreparedStatement preparedStatement, out DuckDBResult result);

        void DuckDBDestroyPrepare(out IntPtr preparedStatement);

        void DuckDBFree(IntPtr ptr);
    }
}