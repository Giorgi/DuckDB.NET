using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace DuckDB.NET.Data.ConnectionString;

internal static class DuckDBConnectionStringParser
{
    public static DuckDBConnectionString Parse(string connectionString)
    {
        var properties = connectionString
            .Split(new[] {';'}, StringSplitOptions.RemoveEmptyEntries)
            .Select(pair => pair.Split(new[] {'='}, 2, StringSplitOptions.RemoveEmptyEntries))
            .ToDictionary(pair => pair[0].Trim(), pair => pair[1].Trim());

        var dataSource = GetDataSource(properties);
            
        if (string.IsNullOrEmpty(dataSource))
        {
            throw new InvalidOperationException($"Connection string '{connectionString}' is not valid.");
        }

        var inMemory = false;
        if (dataSource.Equals(DuckDBConnectionStringBuilder.InMemoryDataSource, StringComparison.OrdinalIgnoreCase))
        {
            inMemory = true;
            dataSource = "";
        }

        var isShared = dataSource.Equals(DuckDBConnectionStringBuilder.InMemorySharedDataSource, StringComparison.OrdinalIgnoreCase);
        if (isShared)
        {
            inMemory = true;
            dataSource = "";
        }
            
        return new DuckDBConnectionString(dataSource, inMemory, isShared);
    }

    private static string GetDataSource(IReadOnlyDictionary<string, string> properties)
    {
        var lowerCaseProperties = properties.ToDictionary(kv => kv.Value.ToLower(CultureInfo.InvariantCulture), kv => kv.Value);
        
        foreach (var key in DuckDBConnectionStringBuilder.DataSourceKeys)
        {
            var lowerKey = key.ToLower(CultureInfo.InvariantCulture);
            
            if (lowerCaseProperties.TryGetValue(lowerKey, out var dataSource))
            {
                return dataSource;
            }
        }
        return null;
    }
}