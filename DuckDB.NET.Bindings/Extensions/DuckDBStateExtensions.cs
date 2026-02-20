namespace DuckDB.NET.Native;

public static class DuckDBStateExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsSuccess(this DuckDBState state)
    {
        return state == DuckDBState.Success;
    }
}