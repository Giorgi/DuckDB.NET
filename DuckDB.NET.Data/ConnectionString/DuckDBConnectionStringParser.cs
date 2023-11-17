using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace DuckDB.NET.Data.ConnectionString;

internal static class DuckDBConnectionStringParser
{
    private static readonly List<string> ConfigurationOptions = new() { "access_mode", "threads", "max_memory" };

    public static DuckDBConnectionString Parse(string connectionString)
    {
        var builder = new DbConnectionStringBuilder
        {
            ConnectionString = connectionString
        };

        var dataSource = GetDataSource(builder);

        if (string.IsNullOrEmpty(dataSource))
        {
            throw new InvalidOperationException($"Connection string '{connectionString}' is not valid, missing data source information.");
        }

        var inMemory = false;

#if NET6_0_OR_GREATER
        if (dataSource.Equals(DuckDBConnectionStringBuilder.InMemoryDataSource, StringComparison.OrdinalIgnoreCase))
#else
        if (dataSource!.Equals(DuckDBConnectionStringBuilder.InMemoryDataSource, StringComparison.OrdinalIgnoreCase))
#endif
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

        var configs = ConfigurationOptions.Where(option => builder.ContainsKey(option))
                                                               .ToDictionary(option => option, option => builder[option].ToString()!);

        return new DuckDBConnectionString(dataSource, inMemory, isShared, configs);
    }

    private static string? GetDataSource(DbConnectionStringBuilder properties)
    {
        foreach (var key in DuckDBConnectionStringBuilder.DataSourceKeys)
        {
            if (properties.TryGetValue(key, out var dataSource))
            {
                return dataSource.ToString();
            }
        }
        return null;
    }
}