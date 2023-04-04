using System.Data.Common;

namespace DuckDB.NET.Data
{
    public class DuckDBConnectionStringBuilder : DbConnectionStringBuilder
    {
        public const string InMemoryDataSource = ":memory:";
        public const string InMemoryConnectionString = "DataSource=:memory:";

        public const string InMemorySharedDataSource = ":memory:?cache=shared";
        public const string InMemorySharedConnectionString = "DataSource=:memory:?cache=shared";
        
        internal static readonly string[] DataSourceKeys = {"Data Source", "DataSource"};
        private const string DataSourceKey = "DataSource";

        private string dataSource = null;
        
        public string DataSource
        {
            get => dataSource;
            set => this[DataSourceKey] = dataSource = value;
        }
    }
}