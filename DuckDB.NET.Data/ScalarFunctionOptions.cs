namespace DuckDB.NET.Data;

public record ScalarFunctionOptions
{
    /// <summary>
    /// Whether the function is pure (deterministic). When null, defaults to
    /// true for functions with parameters and false for parameterless functions.
    /// </summary>
    public bool? IsPureFunction { get; init; }

    /// <summary>
    /// When true, the function receives NULL inputs and handles them itself.
    /// When false (default), DuckDB auto-propagates NULL without calling the function.
    /// </summary>
    public bool HandlesNulls { get; init; }
}
