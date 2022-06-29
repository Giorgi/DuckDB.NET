using DuckDB.NET.Data;
using FluentAssertions;
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
}