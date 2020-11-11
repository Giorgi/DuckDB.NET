using System;
using System.Data.Common;
using System.Runtime.Serialization;

namespace DuckDB.NET.Data
{
    public class DuckDBException : DbException
    {
        public DuckDBException()
        {
        }

        public DuckDBException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }

        public DuckDBException(string message) : base(message)
        {
        }

        public DuckDBException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public DuckDBException(string message, int errorCode) : base(message, errorCode)
        {
        }

        public DuckDBException(string message, DuckDBState state): base(message, (int)state)
        {
            
        }
    }
}