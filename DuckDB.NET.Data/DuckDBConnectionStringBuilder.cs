using DuckDB.NET.Data.ConnectionString;
using DuckDB.NET.Native;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace DuckDB.NET.Data;

public class DuckDBConnectionStringBuilder : DbConnectionStringBuilder
{
    private static readonly HashSet<string> DataSourceKeys = new(StringComparer.OrdinalIgnoreCase) { "Data Source", "DataSource" };
    private static readonly HashSet<string> ConfigurationOptions = new(StringComparer.OrdinalIgnoreCase);

    public const string InMemoryDataSource = ":memory:";
    public const string InMemoryConnectionString = "DataSource=:memory:";

    public const string InMemorySharedDataSource = ":memory:?cache=shared";
    public const string InMemorySharedConnectionString = "DataSource=:memory:?cache=shared";

    private const string DataSourceKey = "DataSource";

    static DuckDBConnectionStringBuilder()
    {
        var configCount = NativeMethods.Configure.DuckDBConfigCount();

        for (var index = 0; index < configCount; index++)
        {
            NativeMethods.Configure.DuckDBGetConfigFlag(index, out var name, out _);
            ConfigurationOptions.Add(name.ToManagedString(false));
        }
    }

    internal static DuckDBConnectionString Parse(string connectionString)
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            ConnectionString = connectionString
        };

        var dataSource = builder.DataSource;

        var configurations = new Dictionary<string, string>();

        foreach (KeyValuePair<string, object> pair in builder)
        {
            if (DuckDBConnectionStringBuilder.DataSourceKeys.Contains(pair.Key))
            {
                continue;
            }

            configurations.Add(pair.Key, pair.Value.ToString()!);
        }

        if (string.IsNullOrEmpty(dataSource))
        {
            throw new InvalidOperationException($"Connection string '{connectionString}' is not valid, missing data source information.");
        }

        var inMemory = dataSource.Equals(InMemoryDataSource, StringComparison.OrdinalIgnoreCase);

        var isShared = dataSource.Equals(DuckDBConnectionStringBuilder.InMemorySharedDataSource, StringComparison.OrdinalIgnoreCase);
        if (isShared)
        {
            inMemory = true;
        }

        return new DuckDBConnectionString(dataSource, inMemory, isShared, configurations);
    }

#if NET6_0_OR_GREATER
    [AllowNull]
#endif
    public override object this[string keyword]
    {
        get => base[keyword];
        set
        {
            if (DataSourceKeys.Contains(keyword) || ConfigurationOptions.Contains(keyword))
            {
                base[keyword] = value;
            }
            else
            {
                throw new InvalidOperationException($"Unrecognized connection string property '{keyword}'");
            }
        }
    }

    public string DataSource
    {
        get
        {
            foreach (var key in DataSourceKeys)
            {
                if (TryGetValue(key, out var value))
                {
                    return value.ToString()!;
                }
            }

            return "";
        }
        set => this[DataSourceKey] = value;
    }
}