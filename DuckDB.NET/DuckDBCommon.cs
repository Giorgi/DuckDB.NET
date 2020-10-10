using System;
using System.Collections.Generic;
using System.Text;

namespace DuckDB.NET
{
    public enum DuckdbState
    {
        DuckDBSuccess = 0,
        DuckDBError = 1
    }

    enum DuckdbType
    {
        DUCKDB_TYPE_INVALID = 0,
        // bool
        DUCKDB_TYPE_BOOLEAN,
        // int8_t
        DUCKDB_TYPE_TINYINT,
        // int16_t
        DUCKDB_TYPE_SMALLINT,
        // int32_t
        DUCKDB_TYPE_INTEGER,
        // int64_t
        DUCKDB_TYPE_BIGINT,
        // float
        DUCKDB_TYPE_FLOAT,
        // double
        DUCKDB_TYPE_DOUBLE,
        // duckdb_timestamp
        DUCKDB_TYPE_TIMESTAMP,
        // duckdb_date
        DUCKDB_TYPE_DATE,
        // duckdb_time
        DUCKDB_TYPE_TIME,
        // duckdb_interval
        DUCKDB_TYPE_INTERVAL,
        // duckdb_hugeint
        DUCKDB_TYPE_HUGEINT,
        // const char*
        DUCKDB_TYPE_VARCHAR
    }

    public struct DuckdbColumn
    {
        IntPtr data;
        bool nullmask;
        DuckdbType type;
        string name;
    }

    public struct DuckdbResult
    {
        public long column_count;
        public long row_count;
        public DuckdbColumn columns;
        public string error_message;
    }
}
