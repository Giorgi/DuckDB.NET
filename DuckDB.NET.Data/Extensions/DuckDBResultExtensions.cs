using DuckDB.NET.Data.Connection;

namespace DuckDB.NET.Data.Extensions;

internal static class DuckDBResultExtensions
{
    // Throws when the result holds an error. A failed execute always has one, so it passes a fallback message
    // for the rare case where DuckDB reports no text. A streaming fetch passes none: duckdb_stream_fetch_chunk
    // returns NULL both at the end of the result and when producing the chunk failed, and only the result's
    // error tells the two apart.
    public static void ThrowOnError(this ref DuckDBResult result, DuckDBNativeConnection connection, string? fallbackMessage = null)
    {
        var errorMessage = NativeMethods.Query.DuckDBResultError(ref result);

        if (string.IsNullOrEmpty(errorMessage))
        {
            if (fallbackMessage is null)
            {
                return;
            }

            errorMessage = fallbackMessage;
        }

        var errorType = NativeMethods.Query.DuckDBResultErrorType(ref result);

        if (errorType == DuckDBErrorType.Interrupt)
        {
            throw new OperationCanceledException();
        }

        var innerException = UdfExceptionStore.Retrieve(connection);
        throw innerException != null
            ? new DuckDBException(errorMessage, innerException)
            : new DuckDBException(errorMessage, errorType);
    }
}
