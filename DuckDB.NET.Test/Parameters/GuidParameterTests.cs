using System;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class GuidParameterTests
{
    [Fact]
    public void SimpleTest()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var guids = new[] { Guid.NewGuid(), Guid.Empty };

        foreach (var guid in guids)
        {
            var command = connection.CreateCommand();
            command.CommandText = $"SELECT '{guid}';";
            command.ExecuteNonQuery();

            var scalar = command.ExecuteScalar();
            scalar.Should().Be(guid.ToString());

            var reader = command.ExecuteReader();
            reader.Read();
            var receivedValue = reader.GetGuid(0);
            receivedValue.Should().Be(guid);
        }
    }

    [Fact]
    public void BindValueTest()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var guids = new[] { Guid.NewGuid(), Guid.Empty };

        foreach (var guid in guids)
        {
            var command = connection.CreateCommand();
            command.CommandText = "SELECT ?;";
            command.Parameters.Add(new DuckDBParameter(guid));
            command.ExecuteNonQuery();

            var scalar = command.ExecuteScalar();
            scalar.Should().Be(guid.ToString());

            var reader = command.ExecuteReader();
            reader.Read();
            var receivedValue = reader.GetGuid(0);
            receivedValue.Should().Be(guid);
        }
    }
}