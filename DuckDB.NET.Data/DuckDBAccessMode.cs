namespace DuckDB.NET.Data;

/// <summary>
/// Access mode of the database, mapped to the DuckDB <c>access_mode</c> configuration option.
/// </summary>
public enum DuckDBAccessMode
{
    /// <summary>
    /// Open the database read/write if possible, otherwise read-only.
    /// </summary>
    Automatic,

    /// <summary>
    /// Open the database in read-only mode. Multiple processes may read the same database file.
    /// </summary>
    ReadOnly,

    /// <summary>
    /// Open the database in read/write mode.
    /// </summary>
    ReadWrite
}
