using System;

namespace DuckDB.NET.Data.Types;

internal interface IDuckDBParameterValue
{
    DuckDBState Bind(DuckDBPreparedStatement preparedStatement, long index);
}