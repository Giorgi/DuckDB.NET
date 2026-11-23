namespace DuckDB.NET.Data;

public class DuckDBException : DbException
{
    public DuckDBErrorType ErrorType { get; }

    /// <summary>
    /// The error code of DuckDB's C API V2, or <see langword="null"/> when the error was not reported through V2.
    /// It is more specific than <see cref="ErrorType"/> for a few errors, such as <see cref="DuckDBErrorCode.ResourceInUse"/>.
    /// </summary>
    public DuckDBErrorCode? NativeErrorCode { get; }

    internal DuckDBException()
    {
    }


    internal DuckDBException(string message) : base(message)
    {
    }

    internal DuckDBException(string message, DuckDBErrorType errorType) : base(message, (int)errorType) => ErrorType = errorType;

    internal DuckDBException(string message, DuckDBErrorCode errorCode) : base(message, (int)errorCode.ToErrorType())
    {
        ErrorType = errorCode.ToErrorType();
        NativeErrorCode = errorCode;
    }

    internal DuckDBException(string message, Exception? innerException) : base(message, innerException)
    {
    }
}