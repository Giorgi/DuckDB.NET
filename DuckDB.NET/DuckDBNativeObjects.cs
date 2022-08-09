using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace DuckDB.NET
{
    public enum DuckDBState
    {
        DuckDBSuccess = 0,
        DuckDBError = 1
    }

    public enum DuckDBType
    {
        DuckdbTypeInvalid = 0,
        // bool
        DuckdbTypeBoolean,
        // int8_t
        DuckdbTypeTinyInt,
        // int16_t
        DuckdbTypeSmallInt,
        // int32_t
        DuckdbTypeInteger,
        // int64_t
        DuckdbTypeBigInt,
        // uint8_t
        DuckdbTypeUnsignedTinyInt,
        // uint16_t
        DuckdbTypeUnsignedSmallInt,
        // uint32_t
        DuckdbTypeUnsignedInteger,
        // uint64_t
        DuckdbTypeUnsignedBigInt,
        // float
        DuckdbTypeFloat,
        // double
        DuckdbTypeDouble,
        // duckdb_timestamp
        DuckdbTypeTimestamp,
        // duckdb_date
        DuckdbTypeDate,
        // duckdb_time
        DuckdbTypeTime,
        // duckdb_interval
        DuckdbTypeInterval,
        // duckdb_hugeint
        DuckdbTypeHugeInt,
        // const char*
        DuckdbTypeVarchar,
        // duckdb_blob
        DuckdbTypeBlob,
        DuckdbTypeDecimal,
    }

    [StructLayout(LayoutKind.Sequential)]
    public class DuckDBResult: IDisposable
    {
        [Obsolete]
        private long ColumnCount;

        [Obsolete]
        private long RowCount;

        [Obsolete]
        private long RowsChanged;

        [Obsolete]
        private IntPtr columns;

        [Obsolete]
        private IntPtr ErrorMessage;

        private IntPtr internal_data;
        
        public void Dispose()
        {
            NativeMethods.Query.DuckDBDestroyResult(this);
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBDateStruct
    {
        public int Year { get; set; }

        public byte Month { get; set; }

        public byte Day { get; set; }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBDate
    {
        public int Days { get; set; }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBTimeStruct
    {
        public byte Hour { get; set; }

        public byte Min { get; set; }

        public byte Sec { get; set; }

        public int Msec { get; set; }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBTime
    {
        public long Micros { get; set; }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBTimestampStruct
    {
        public DuckDBDateStruct Date { get; set; }
        public DuckDBTimeStruct Time { get; set; }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBTimestamp
    {
        public long Micros { get; set; }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBBlob : IDisposable
    {
        public IntPtr Data { get; }

        public long Size { get;}

        public void Dispose()
        {
            NativeMethods.Helpers.DuckDBFree(Data);
        }
    }
}
