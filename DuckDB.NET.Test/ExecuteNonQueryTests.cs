using FluentAssertions;
using System.Collections.Generic;
using Xunit;

namespace DuckDB.NET.Test;

public class ExecuteNonQueryTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void TableQueryTest()
    {
        Command.CommandText = "CREATE TABLE users (id INTEGER, name TEXT);";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO users VALUES (1, 'user1'), (2, 'user2'), (3, 'user3'), (4, 'user4');";
        var affectedRows = Command.ExecuteNonQuery();
        affectedRows.Should().Be(4);

        Command.CommandText = "UPDATE users SET name = 'unnamed' WHERE id % 2 = 0;";
        affectedRows = Command.ExecuteNonQuery();
        affectedRows.Should().Be(2);
    }

    [Fact]
    public void ExecuteQueryWithUnicodeText()
    {
        var words = new List<string> { "张三", "李四", "王五", "გიორგი" };

        Command.CommandText = "CREATE TABLE test(id BIGINT, name STRING); ";
        Command.ExecuteNonQuery();

        Command.CommandText = $"INSERT INTO test VALUES (1, '{words[0]}'), (3, '{words[1]}'), (5, '{words[2]}'), (4, '{words[3]}')";
        Command.ExecuteNonQuery().Should().Be(4);

        Command.CommandText = "Select * from test";
        using (var reader = Command.ExecuteReader())
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