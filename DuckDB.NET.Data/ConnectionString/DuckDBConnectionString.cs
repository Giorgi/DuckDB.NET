using System.Collections.Generic;

namespace DuckDB.NET.Data.ConnectionString;

internal class DuckDBConnectionString
{
    public string DataSource { get; }
    public bool InMemory { get; }
    public bool Shared { get; }
    public IReadOnlyDictionary<string, string> Configuration { get; }

    public DuckDBConnectionString(string dataSource, bool inMemory, bool shared, IReadOnlyDictionary<string, string> configuration)
    {
        DataSource = dataSource;
        InMemory = inMemory;
        Shared = shared;
        Configuration = configuration;
    }
}