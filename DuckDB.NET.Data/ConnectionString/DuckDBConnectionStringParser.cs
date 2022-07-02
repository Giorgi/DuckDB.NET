using System;
using System.Collections.Generic;
using System.Linq;

namespace DuckDB.NET.Data.ConnectionString
{
    internal static class DuckDBConnectionStringParser
    {
        private static readonly HashSet<string> DataSourceKeys 
            = new HashSet<string>(new []{"Data Source", "DataSource"}, StringComparer.OrdinalIgnoreCase);
        
        public static IDuckDBConnectionString Parse(string connectionString)
        {
            return new DuckDBConnectionString(GetFileName(connectionString));
        }
        
        private static string GetFileName(string connectionString)
        {
            var connectionStringParts = connectionString.Split('=').Select(x => x.Trim()).ToArray();
            if (connectionStringParts.Length != 2)
            {
                throw new InvalidOperationException($"ConnectionString '{connectionString}' is not valid");
            }

            if (!DataSourceKeys.Contains(connectionStringParts[0]))
            {
                throw new InvalidOperationException($"ConnectionString '{connectionString}' is not valid");
            }

            return IsInMemoryDataSource(connectionStringParts[1])
                ? string.Empty
                : connectionStringParts[1];
        }
        
        private static bool IsInMemoryDataSource(string dataSource) => dataSource.Equals(DuckDBConnectionStringBuilder.InMemory, StringComparison.OrdinalIgnoreCase);
    }
}