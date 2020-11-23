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
        DuckdbTypeVarchar
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBColumn
    {
        IntPtr data;
        IntPtr nullmask;

        public DuckDBType Type { get; }
        public string Name { get; }

        public bool NullMask(int row) => Marshal.ReadByte(nullmask + row) != 0;
        public IntPtr Data => data;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBResult
    {
        public long ColumnCount { get; }
        public long RowCount { get; }

        private IntPtr columns;

        public string ErrorMessage { get; }

        public IReadOnlyList<DuckDBColumn> Columns
        {
            get
            {
                var result = new List<DuckDBColumn>();

                for (int i = 0; i < ColumnCount; i++)
                {
                    var column = Marshal.PtrToStructure<DuckDBColumn>(columns + Marshal.SizeOf<DuckDBColumn>() * i);
                    result.Add(column);
                }

                return result.AsReadOnly();
            }
        }
    }

    public struct DuckDBDate
    {
        public int Year { get; }

        public byte Month { get; }

        public byte Day { get; }
    }
}
