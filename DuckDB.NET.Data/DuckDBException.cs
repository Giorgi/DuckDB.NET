using System.Data.Common;
using System.Runtime.Serialization;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data;

public class DuckDBException : DbException
{
    internal DuckDBException()
    {
    }

    internal DuckDBException(SerializationInfo info, StreamingContext context) : base(info, context)
    {
    }

    internal DuckDBException(string message) : base(message)
    {
    }

    internal DuckDBException(string message, DuckDBState state) : base(message, (int)state)
    {

    }
}