using DuckDB.NET.Data;
using FluentAssertions;
using System.Collections.Generic;
using Xunit;

namespace DuckDB.NET.Test;

public class ExecuteNonQueryTests
{
    [Fact]
    public void TableQueryTest()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE users (id INTEGER, name TEXT);";
        command.ExecuteNonQuery();

        command.CommandText = "INSERT INTO users VALUES (1, 'user1'), (2, 'user2'), (3, 'user3'), (4, 'user4');";
        var affectedRows = command.ExecuteNonQuery();
        affectedRows.Should().Be(4);

        command.CommandText = "UPDATE users SET name = 'unnamed' WHERE id % 2 = 0;";
        affectedRows = command.ExecuteNonQuery();
        affectedRows.Should().Be(2);
    }

    [Fact]
    public void ExecuteQueryWithUnicodeText()
    {
        var words = new List<string> { "张三", "李四", "王五", "გიორგი" };

        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE test(id BIGINT, name STRING); ";
        command.ExecuteNonQuery();

        command.CommandText = $"INSERT INTO test VALUES (1, '{words[0]}'), (3, '{words[1]}'), (5, '{words[2]}'), (4, '{words[3]}')";
        command.ExecuteNonQuery().Should().Be(4);

        command.CommandText = "Select * from test";
        using (var reader = command.ExecuteReader())
        {
            var results = new List<string>();
            while (reader.Read())
            {
                var text = reader.IsDBNull(1) ? null : reader.GetString(1);
                results.Add(text);
            }

            results.Should().BeEquivalentTo(words);
        }
    }
}