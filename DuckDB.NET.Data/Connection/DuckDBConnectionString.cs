namespace DuckDB.NET.Data.Connection;

internal class DuckDBConnectionString(string dataSource, IReadOnlyDictionary<string, string> configuration, string? instancePath)
{
    /// <summary>The path DuckDB opens: a file, a named in-memory database (':memory:name'), or null for a private one.</summary>
    public string? InstancePath { get; } = instancePath;

    public string DataSource { get; } = dataSource;
    public IReadOnlyDictionary<string, string> Configuration { get; } = configuration;
}