namespace DuckDB.NET.Data.ConnectionString
{
    internal interface IDuckDBConnectionString
    {
        string DataSource { get; }
    }

    internal class DuckDBConnectionString : IDuckDBConnectionString
    {
        public string DataSource { get; }

        public DuckDBConnectionString(string dataSource)
        {
            DataSource = dataSource;
        }
    }
}