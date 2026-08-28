namespace DuckDB.NET.Data;

/// <summary>
/// NULL ordering used when an <c>ORDER BY</c> clause does not specify one,
/// mapped to the DuckDB <c>default_null_order</c> configuration option.
/// </summary>
public enum DuckDBNullOrder
{
    /// <summary>
    /// NULL values are ordered first, whatever the sort direction (<c>NULLS_FIRST</c>).
    /// </summary>
    NullsFirst,

    /// <summary>
    /// NULL values are ordered last, whatever the sort direction (<c>NULLS_LAST</c>).
    /// </summary>
    NullsLast,

    /// <summary>
    /// NULL values are ordered first when sorting ascending and last when sorting descending
    /// (<c>NULLS_FIRST_ON_ASC_LAST_ON_DESC</c>). This is how SQLite and MySQL order NULLs.
    /// </summary>
    NullsFirstOnAscLastOnDesc,

    /// <summary>
    /// NULL values are ordered last when sorting ascending and first when sorting descending
    /// (<c>NULLS_LAST_ON_ASC_FIRST_ON_DESC</c>). This is how PostgreSQL orders NULLs.
    /// </summary>
    NullsLastOnAscFirstOnDesc
}
