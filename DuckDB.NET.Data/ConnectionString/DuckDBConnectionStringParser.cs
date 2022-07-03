using System;
using System.Collections.Generic;
using System.Linq;

namespace DuckDB.NET.Data.ConnectionString
{
    internal static class DuckDBConnectionStringParser
    {
        public static DuckDBConnectionString Parse(string connectionString)
        {
            var properties = connectionString
                .Split(new[] {';'}, StringSplitOptions.RemoveEmptyEntries)
                .Select(pair => pair.Split(new[] {'='}, StringSplitOptions.RemoveEmptyEntries))
                .ToDictionary(pair => pair[0].Trim(), pair => pair[1].Trim());

            var dataSource = GetDataSource(properties);
            
            if (string.IsNullOrEmpty(dataSource))
            {
                throw new InvalidOperationException($"Connection string '{connectionString}' is not valid.");
            }

            if (dataSource.Equals(DuckDBConnectionStringBuilder.InMemoryDataSource, StringComparison.OrdinalIgnoreCase))
            {
                dataSource = string.Empty;
            }
            
            return new DuckDBConnectionString(dataSource);
        }

        private static string GetDataSource(IReadOnlyDictionary<string, string> properties)
        {
            foreach (var key in DuckDBConnectionStringBuilder.DataSourceKeys)
            {
                if (properties.TryGetValue(key, out var dataSource))
                {
                    return dataSource;
                }
            }
            return null;
        }
    }
}