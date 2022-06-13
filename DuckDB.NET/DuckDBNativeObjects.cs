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
        DuckdbTypeBlob
    }

    [StructLayout(LayoutKind.Sequential)]
    public class DuckDBResult
    {
        // Deprecated
        private long ColumnCount;
        // Deprecated
        private long RowCount;
        // Deprecated
        private long RowsChanged;
        // Deprecated
        private IntPtr columns;
        // Deprecated
        private string ErrorMessage;

        private IntPtr internal_data;
    }

    public struct DuckDBDate
    {
        public int Year { get; }

        public byte Month { get; }

        public byte Day { get; }
    }

    public struct DuckDBTime
    {
        public byte Hour { get; }

        public byte Min { get; }

        public byte Sec { get; }

        public short Msec { get; }
    }

    public struct DuckDBTimestamp
    {
        public DuckDBDate Date { get; }

        public DuckDBTime Time { get; }
    }
}
