using System;
using System.Collections.Generic;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class QueryScalarTests
{
    [Fact]
    public void SimpleQueryTest()
    {
        var queries = new Dictionary<string, object>
        {
            {"SELECT 42;", 42},
            {"SELECT 'test';", "test"},
            {"SELECT sin(1);", Math.Sin(1)},
            {"SELECT sin(10);", Math.Sin(10)},
        };
            
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var command = connection.CreateCommand();
            
        foreach (var (query, expectedResult) in queries)
        {
            command.CommandText = query;
            var scalar = command.ExecuteScalar();

            scalar.Should().Be(expectedResult);
        }
    }

    [Fact]
    public void TableQueryTest()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE scalarUsers (id INTEGER, name TEXT);";
        command.ExecuteNonQuery();

        command.CommandText = "INSERT INTO scalarUsers VALUES (1, 'user1'), (2, 'user2'), (3, 'user3');";
        var affectedRows = command.ExecuteNonQuery();
        affectedRows.Should().Be(3);

        command.CommandText = "SELECT name FROM scalarUsers LIMIT 1;";
        var scalar = command.ExecuteScalar();
        scalar.Should().Be("user1");
            
        command.CommandText = "SELECT name, id FROM scalarUsers WHERE id = 1;";
        scalar = command.ExecuteScalar();
        scalar.Should().Be("user1");
    }
}