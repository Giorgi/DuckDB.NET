namespace DuckDB.NET.Data.ConnectionString
{
    internal class DuckDBConnectionString
    {
        public string DataSource { get; }

        public DuckDBConnectionString(string dataSource)
        {
            DataSource = dataSource;
        }
    }
}