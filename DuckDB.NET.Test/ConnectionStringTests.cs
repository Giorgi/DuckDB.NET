using System;
using System.Data;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class ConnectionStringTests
{
    [Theory]
    [InlineData(DuckDBConnectionStringBuilder.InMemoryConnectionString)]
    [InlineData("DataSource = :memory:")]
    [InlineData("Data Source=:memory:")]
    [InlineData("Data Source = :memory:")]
    [InlineData("DataSource=   :memory:")]
    [InlineData("Data Source=    :memory:")]
    [InlineData("DataSource   =   :memory:")]
    [InlineData("Data Source    =    :memory:")]
    [InlineData("DataSource   =:memory:")]
    [InlineData("Data Source    =:memory:")]
    [InlineData("DataSource=:Memory:")]
    [InlineData("Data Source=:Memory:")]
    [InlineData("datasource=:memory:")]
    [InlineData("data source=:memory:")]
    [InlineData("daTa source=:memory:")]
    public void ExplicitConnectionStringTest(string connectionString)
    {
        using var connection = new DuckDBConnection(connectionString);
        connection.Open();

        connection.State.Should().Be(ConnectionState.Open);
    }

    [Theory]
    [InlineData("DataSource = ")]
    [InlineData("Source=:memory:")]
    [InlineData("Data=:memory:")]
    [InlineData("DataSource = :memory:;Something=else")]
    public void InvalidConnectionStringTests(string connectionString)
    {
        using var connection = new DuckDBConnection(connectionString);
        connection.Invoking(con => con.Open())
            .Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ConnectionStringBuilderDataSourceTest()
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource
        };

        using var connection = new DuckDBConnection(builder.ToString());
        connection.Open();

        connection.Database.Should().Be(DuckDBConnectionStringBuilder.InMemoryDataSource);
        connection.DataSource.Should().Be(DuckDBConnectionStringBuilder.InMemoryDataSource);
        connection.State.Should().Be(ConnectionState.Open);
    }

    [Fact]
    public void ConnectionStringBuilderSetPropertiesTest()
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource,
            ["threads"] = 8,
            ["ACCESS_MODE"] = "automatic"
        };

        builder.ConnectionString.Should().Be("DataSource=:memory:;threads=8;ACCESS_MODE=automatic");
    }

    [Fact]
    public void ConnectionStringBuilderSetNotExistingProperty()
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource,
        };

        builder.Invoking(b => b["dummy"] = "prop").Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ConnectionStringBuilderGetPropertiesTest()
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            ConnectionString = "DataSource = :memory:;Threads = 8;ACCESS_MODE=automatic"
        };

        builder.DataSource.Should().Be(DuckDBConnectionStringBuilder.InMemoryDataSource);
        builder["threads"].Should().Be("8");
        builder["access_mode"].Should().Be("automatic");
    }
}