using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace DuckDB.NET.Data.ConnectionString;

internal static class DuckDBConnectionStringParser
{
    private static readonly HashSet<string> ConfigurationOptions = new(StringComparer.OrdinalIgnoreCase);

    static DuckDBConnectionStringParser()
    {
        var configCount = NativeMethods.Configure.DuckDBConfigCount();

        for (var index = 0; index < configCount; index++)
        {
            NativeMethods.Configure.DuckDBGetConfigFlag(index, out var name, out _);
            ConfigurationOptions.Add(name.ToManagedString(false));
        }
    }

    public static DuckDBConnectionString Parse(string connectionString)
    {
        var builder = new DbConnectionStringBuilder
        {
            ConnectionString = connectionString
        };

        var dataSource = "";

        var configurations = new Dictionary<string, string>();

        foreach (KeyValuePair<string, object> pair in builder)
        {
            if (DuckDBConnectionStringBuilder.DataSourceKeys.Contains(pair.Key, StringComparer.OrdinalIgnoreCase))
            {
                dataSource = pair.Value.ToString();
            }
            else
            {
                if (ConfigurationOptions.Contains(pair.Key))
                {
                    configurations.Add(pair.Key, pair.Value.ToString()!);
                }
                else
                {
                    throw new InvalidOperationException($"Unrecognized connection string property '{pair.Key}'");
                }
            }
        }
        
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

        return new DuckDBConnectionString(dataSource, inMemory, isShared, configurations);
    }
}