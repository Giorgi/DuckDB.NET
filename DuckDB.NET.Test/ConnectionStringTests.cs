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
    public void ExplicitConnectionStringTest(string connectionString)
    {
        using var connection = new DuckDBConnection(connectionString);
        connection.Open();

        connection.State.Should().Be(ConnectionState.Open);
    }
        
    [Theory]
    [InlineData("Source=:memory:")]
    [InlineData("Data=:memory:")]
    public void InvalidConnectionStringTests(string connectionString)
    {
        using var connection = new DuckDBConnection(connectionString);
        connection.Invoking(con => con.Open())
            .Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ConnectionStringBuilderTest()
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource
        };

        using var connection = new DuckDBConnection(builder.ToString());
        connection.Open();
        connection.State.Should().Be(ConnectionState.Open);
    }
}