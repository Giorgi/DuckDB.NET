using System;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class GuidParameterTests : DuckDBTestBase
{
    public GuidParameterTests(DuckDBDatabaseFixture db) : base(db)
    {
    }

    [Fact]
    public void SimpleTest()
    {
        var guids = new[] { Guid.NewGuid(), Guid.Empty };

        foreach (var guid in guids)
        {
            Command.CommandText = $"SELECT '{guid}'::uuid;";

            var scalar = Command.ExecuteScalar();
            scalar.Should().Be(guid);

            var reader = Command.ExecuteReader();
            reader.Read();

            var receivedValue = reader.GetGuid(0);
            receivedValue.Should().Be(guid);
        }
    }

    [Fact]
    public void BindValueTest()
    {
        var guids = new[] { Guid.NewGuid(), Guid.Empty };

        foreach (var guid in guids)
        {
            Command.CommandText = "SELECT ?::uuid;";

            Command.Parameters.Clear();
            Command.Parameters.Add(new DuckDBParameter(guid));
            Command.ExecuteNonQuery();

            var scalar = Command.ExecuteScalar();
            scalar.Should().Be(guid);

            var reader = Command.ExecuteReader();
            reader.Read();
            var receivedValue = reader.GetGuid(0);
            receivedValue.Should().Be(guid);
        }
    }
}