namespace DuckDB.NET.Data.Connection;

internal class DuckDBConnectionString(string dataSource, bool inMemory, bool shared, IReadOnlyDictionary<string, string> configuration,
                                      string? instancePath)
{
    /// <summary>The path DuckDB opens: a file, a named in-memory database (':memory:name'), or null for a private one.</summary>
    public string? InstancePath { get; } = instancePath;

    public string DataSource { get; } = dataSource;
    public bool InMemory { get; } = inMemory;
    public bool Shared { get; } = shared;
    public IReadOnlyDictionary<string, string> Configuration { get; } = configuration;
}