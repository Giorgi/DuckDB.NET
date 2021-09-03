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

        public T ReadAs<T>(int row) where T : struct
        {
            return Marshal.PtrToStructure<T>(Data + Marshal.SizeOf<T>() * row);
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct DuckDBResult
    {
        public long ColumnCount { get; }
        public long RowCount { get; }
        public long RowsChanged { get; }

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
