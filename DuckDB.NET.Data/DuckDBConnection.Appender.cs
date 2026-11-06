namespace DuckDB.NET.Data;

partial class DuckDBConnection
{
    public DuckDBAppender CreateAppender(string table) => CreateAppender(null, null, table);

    public DuckDBAppender CreateAppender(string? schema, string table) => CreateAppender(null, schema, table);

    public DuckDBAppender CreateAppender(string? catalog, string? schema, string table)
    {
        EnsureConnectionOpen();

        var appenderState = NativeMethods.Appender.DuckDBAppenderCreateExt(NativeConnection, catalog, schema, table, out var nativeAppender);

        if (!appenderState.IsSuccess())
        {
            try
            {
                NativeMethods.Appender.DuckDBAppenderErrorData(nativeAppender).ThrowOnError();
            }
            finally
            {
                nativeAppender.Close();
            }
        }

        return new DuckDBAppender(nativeAppender, GetTableName());

        string GetTableName()
        {
            return string.IsNullOrEmpty(schema) ? table : $"{schema}.{table}";
        }
    }

    /// <summary>
    /// Creates a type-safe appender using an AppenderMap for property-to-column mappings.
    /// </summary>
    /// <typeparam name="T">The type to append</typeparam>
    /// <typeparam name="TMap">The AppenderMap type defining the mappings</typeparam>
    /// <param name="table">The table name</param>
    /// <returns>A type-safe mapped appender</returns>
    public DuckDBMappedAppender<T, TMap> CreateAppender<T, TMap>(string table) 
        where TMap : Mapping.DuckDBAppenderMap<T>, new()
    {
        return CreateAppender<T, TMap>(null, null, table);
    }

    /// <summary>
    /// Creates a type-safe appender using an AppenderMap for property-to-column mappings.
    /// </summary>
    /// <typeparam name="T">The type to append</typeparam>
    /// <typeparam name="TMap">The AppenderMap type defining the mappings</typeparam>
    /// <param name="schema">The schema name</param>
    /// <param name="table">The table name</param>
    /// <returns>A type-safe mapped appender</returns>
    public DuckDBMappedAppender<T, TMap> CreateAppender<T, TMap>(string? schema, string table)
        where TMap : Mapping.DuckDBAppenderMap<T>, new()
    {
        return CreateAppender<T, TMap>(null, schema, table);
    }

    /// <summary>
    /// Creates a type-safe appender using an AppenderMap for property-to-column mappings.
    /// </summary>
    /// <typeparam name="T">The type to append</typeparam>
    /// <typeparam name="TMap">The AppenderMap type defining the mappings</typeparam>
    /// <param name="catalog">The catalog name</param>
    /// <param name="schema">The schema name</param>
    /// <param name="table">The table name</param>
    /// <returns>A type-safe mapped appender</returns>
    public DuckDBMappedAppender<T, TMap> CreateAppender<T, TMap>(string? catalog, string? schema, string table)
        where TMap : Mapping.DuckDBAppenderMap<T>, new()
    {
        var appender = CreateAppender(catalog, schema, table);

        try
        {
            return new DuckDBMappedAppender<T, TMap>(appender);
        }
        catch
        {
            // The mapping did not pass validation, so nobody else gets to dispose the appender.
            appender.Dispose();
            throw;
        }
    }
}
