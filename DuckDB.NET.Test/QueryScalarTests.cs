using System;
using System.Collections.Generic;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class QueryScalarTests : DuckDBTestBase
{
    public QueryScalarTests(DuckDBDatabaseFixture db) : base(db)
    {
    }

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

        foreach (var (query, expectedResult) in queries)
        {
            Command.CommandText = query;
            var scalar = Command.ExecuteScalar();

            scalar.Should().Be(expectedResult);
        }
    }

    [Fact]
    public void TableQueryTest()
    {
        Command.CommandText = "CREATE TABLE scalarUsers (id INTEGER, name TEXT);";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO scalarUsers VALUES (1, 'user1'), (2, 'user2'), (3, 'user3');";
        var affectedRows = Command.ExecuteNonQuery();
        affectedRows.Should().Be(3);

        Command.CommandText = "SELECT name FROM scalarUsers LIMIT 1;";
        var scalar = Command.ExecuteScalar();
        scalar.Should().Be("user1");

        Command.CommandText = "SELECT name, id FROM scalarUsers WHERE id = 1;";
        scalar = Command.ExecuteScalar();
        scalar.Should().Be("user1");

        Command.CommandText = "Select id from scalarUsers where 1=2";
        scalar = Command.ExecuteScalar();
        scalar.Should().BeNull();
    }
}