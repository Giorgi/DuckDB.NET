using DuckDB.NET.Native;
using System.Data.Common;
using System.Runtime.Serialization;

namespace DuckDB.NET.Data;

public class DuckDBException : DbException
{
    public DuckDBErrorType ErrorType { get; }

    internal DuckDBException()
    {
    }

#if !NET8_0_OR_GREATER
    internal DuckDBException(SerializationInfo info, StreamingContext context) : base(info, context)
    {
    }
#endif

    internal DuckDBException(string message) : base(message)
    {
    }

    internal DuckDBException(string message, DuckDBErrorType errorType) : base(message, (int)errorType)
    {
        ErrorType = errorType;
    }
}