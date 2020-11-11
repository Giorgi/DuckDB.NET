namespace DuckDB.NET.Data
{
    static class Utils
    {
        internal static bool IsSuccess(this DuckDBState duckDBState)
        {
            return duckDBState == DuckDBState.DuckDBSuccess;
        }
    }
}