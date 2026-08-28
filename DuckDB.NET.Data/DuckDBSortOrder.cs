namespace DuckDB.NET.Data;

/// <summary>
/// Sort order used when an <c>ORDER BY</c> clause does not specify one,
/// mapped to the DuckDB <c>default_order</c> configuration option.
/// </summary>
public enum DuckDBSortOrder
{
    /// <summary>
    /// Sort ascending (<c>ASC</c>).
    /// </summary>
    Ascending,

    /// <summary>
    /// Sort descending (<c>DESC</c>).
    /// </summary>
    Descending
}
