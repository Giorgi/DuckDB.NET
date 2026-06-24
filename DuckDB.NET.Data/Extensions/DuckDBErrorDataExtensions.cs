namespace DuckDB.NET.Data.Extensions;

internal static class DuckDBErrorDataExtensions
{
    public static void ThrowOnError(this DuckDBErrorData errorData, string message)
    {
        using (errorData)
        {
            if (errorData.HasError)
            {
                throw new DuckDBException($"{message} {errorData.Message}".TrimEnd());
            }
        }
    }
}
