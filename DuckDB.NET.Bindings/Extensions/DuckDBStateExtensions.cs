namespace DuckDB.NET.Native;

public static class DuckDBStateExtensions
{
    public static bool IsSuccess(this DuckDBState state)
    {
        return state == DuckDBState.Success;
    }
}