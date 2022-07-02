using System;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test
{
    public class ConnectionStringTests
    {
        [Theory]
        [InlineData("DataSource=:memory:")]
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
        public void ExplicitConnectionStringTest(string connectionString)
        {
            using var connection = new DuckDBConnection(connectionString);
            connection.Open();
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
                DataSource = DuckDBConnectionStringBuilder.InMemory
            };

            using var connection = new DuckDBConnection(builder.ToString());
            connection.Open();

            using var connection2 = new DuckDBConnection(builder);
            connection2.Open();
        }
    }
}