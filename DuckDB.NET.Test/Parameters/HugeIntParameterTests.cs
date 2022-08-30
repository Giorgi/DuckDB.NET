using System.Numerics;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class HugeIntParameterTests
{
    [Fact]
    public void SimpleTest()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = "SELECT 125::HUGEINT;";
        command.ExecuteNonQuery();

        var scalar = command.ExecuteScalar();
        scalar.Should().Be(new BigInteger(125));

        var reader = command.ExecuteReader();
        reader.Read();
        var receivedValue = reader.GetFieldValue<BigInteger>(0);
        receivedValue.Should().Be(125);

        reader.GetFieldType(0).Should().Be(typeof(BigInteger));
    }

    [Fact]
    public void BindValueTest()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "CREATE TABLE HugeIntTests (key INTEGER, value HugeInt)";
        duckDbCommand.ExecuteNonQuery();

        duckDbCommand.CommandText = "INSERT INTO HugeIntTests VALUES (9, ?);";
        
        var value = BigInteger.Add(ulong.MaxValue, 125);
        duckDbCommand.Parameters.Add(new DuckDBParameter(value));
        duckDbCommand.ExecuteNonQuery();

        var command = connection.CreateCommand();
        command.CommandText = "SELECT * from HugeIntTests;";
        
        var reader = command.ExecuteReader();
        reader.Read();
        
        var receivedValue = reader.GetFieldValue<BigInteger>(1);
        receivedValue.Should().Be(value);
    }
}