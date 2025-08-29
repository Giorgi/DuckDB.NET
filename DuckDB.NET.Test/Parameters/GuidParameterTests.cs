using System;
using System.Data;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class GuidParameterTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void ReadGuid()
    {
        var guids = new[] { Guid.NewGuid(), Guid.Empty };

        foreach (var guid in guids)
        {
            Command.CommandText = $"SELECT '{guid}'::uuid;";

            var scalar = Command.ExecuteScalar();
            scalar.Should().Be(guid);

            var reader = Command.ExecuteReader();
            reader.Read();

            reader.GetFieldType(0).Should().Be(typeof(Guid));

            var receivedValue = reader.GetGuid(0);
            receivedValue.Should().Be(guid);
        }
    }

    [Fact]
    public void ReadGuidNullable()
    {
        Command.CommandText = "SELECT ?::uuid;";
        Command.Parameters.Add(new DuckDBParameter(DbType.Guid, null));

        var reader = Command.ExecuteReader();
        reader.Read();

        var receivedValue = reader.GetFieldValue<Guid?>(0);
        receivedValue.Should().BeNull();

        reader.Invoking(r => r.GetFieldValue<Guid>(0)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void InsertGuidSelect()
    {
        Command.CommandText = "CREATE TABLE uuid_test (a uuid);";
        Command.ExecuteNonQuery();

        var value = Guid.NewGuid();

        Command.CommandText = "INSERT INTO uuid_test (a) VALUES (?);";
        Command.Parameters.Add(new DuckDBParameter(value));
        Command.ExecuteNonQuery();

        Command.CommandText = "SELECT * FROM uuid_test;";
        var reader = Command.ExecuteReader();
        reader.Read();

        var guid = reader.GetFieldValue<Guid>(0);
        guid.Should().Be(value);
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

    [Fact]
    public void BindParameterWithoutTable()
    {
        var value = Guid.NewGuid();
        
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));

        using var reader = Command.ExecuteReader();
        reader.Read();
        var result = reader.GetFieldValue<Guid>(0);

        result.Should().Be(value);
    }
}