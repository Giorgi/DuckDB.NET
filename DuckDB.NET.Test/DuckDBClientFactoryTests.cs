using System.Data.Common;
using System.Diagnostics;
using DuckDB.NET.Test.Helpers;

namespace DuckDB.NET.Test;

public class DuckDBClientFactoryTests
{
    [Fact]
    public void RegisterFactory()
    {
        DbProviderFactories.RegisterFactory(DuckDBClientFactory.ProviderInvariantName, DuckDBClientFactory.Instance);
        DbProviderFactories.TryGetFactory(DuckDBClientFactory.ProviderInvariantName, out var factory);

        Assert.NotNull(factory);
        Assert.IsType<DuckDBClientFactory>(factory);
    }

    [Fact]
    public void UseFactory()
    {
        DbProviderFactories.RegisterFactory(DuckDBClientFactory.ProviderInvariantName, DuckDBClientFactory.Instance);
        DbProviderFactories.TryGetFactory(DuckDBClientFactory.ProviderInvariantName, out var factory);

        Assert.NotNull(factory);
            
        using var connection = factory.CreateConnection();
        using var command = factory.CreateCommand();
        var parameter = factory.CreateParameter();

        var connectionStringBuilder = factory.CreateConnectionStringBuilder();
        connectionStringBuilder["DataSource"] = DuckDBConnectionStringBuilder.InMemoryDataSource;
            
        connection.ConnectionString = connectionStringBuilder.ConnectionString;

        command.CommandText = "Select ?::integer";
        command.Connection = connection;
        parameter.Value = 42;
        command.Parameters.Add(parameter);
            
        connection.Open();

        using var reader = command.ExecuteReader();
        reader.Read();

        var value = reader.GetInt32(0);
            
        Assert.Equal(42, value);
    }

    [Fact]
    public void FactoryReportsWhatItCannotCreate()
    {
        DbProviderFactory factory = DuckDBClientFactory.Instance;

        // Generic data code asks these before creating anything, so they must answer, not throw.
        Assert.False(factory.CanCreateDataAdapter);
        Assert.False(factory.CanCreateCommandBuilder);
        Assert.False(factory.CanCreateDataSourceEnumerator);

        Assert.Null(factory.CreateDataAdapter());
        Assert.Null(factory.CreateCommandBuilder());
    }
}