namespace DuckDB.NET.Data.Extensions;

internal static class DuckDBErrorDataExtensions
{
    public static void ThrowOnError(this DuckDBErrorData errorData, string message = "")
    {
        using (errorData)
        {
            if (errorData.HasError)
            {
                var errorType = NativeMethods.ErrorData.DuckDBErrorDataErrorType(errorData);
                throw new DuckDBException($"{message} {errorData.Message}".Trim(), errorType);
            }
        }
    }
}
