namespace DuckDB.NET.Data.Connection;

internal class DuckDBConnectionString(string dataSource, bool inMemory, bool shared, IReadOnlyDictionary<string, string> configuration)
{
    public string DataSource { get; } = dataSource;
    public bool InMemory { get; } = inMemory;
    public bool Shared { get; } = shared;
    public IReadOnlyDictionary<string, string> Configuration { get; } = configuration;
}