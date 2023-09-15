namespace DuckDB.NET.Data.ConnectionString;

internal class DuckDBConnectionString
{
    public string DataSource { get; }
    public bool InMemory { get; set; }
    public bool Shared { get; set; }

    public DuckDBConnectionString(string dataSource, bool inMemory = false, bool shared = false)
    {
        DataSource = dataSource;
        InMemory = inMemory;
        Shared = shared;
    }
}