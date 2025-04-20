using System.Numerics;
using DuckDB.NET.Data;
using DuckDB.NET.Native;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class HugeIntParameterTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void SimpleTest()
    {
        Command.CommandText = "SELECT 125::HUGEINT;";
        Command.ExecuteNonQuery();

        var scalar = Command.ExecuteScalar();
        scalar.Should().Be(new BigInteger(125));

        using var reader = Command.ExecuteReader();
        reader.Read();
        var receivedValue = reader.GetFieldValue<BigInteger>(0);
        receivedValue.Should().Be(125);

        reader.GetFieldValue<sbyte>(0).Should().Be(125);
        reader.GetFieldValue<short>(0).Should().Be(125);
        reader.GetFieldValue<int>(0).Should().Be(125);
        reader.GetFieldValue<long>(0).Should().Be(125);
        reader.GetFieldValue<uint>(0).Should().Be(125);
        reader.GetFieldValue<ulong>(0).Should().Be(125);

        reader.GetFieldType(0).Should().Be(typeof(BigInteger));
    }

    [Fact]
    public void BindValueTest()
    {
        Command.CommandText = "CREATE TABLE HugeIntTests (key INTEGER, value HugeInt)";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO HugeIntTests VALUES (9, ?);";

        var value = BigInteger.Add(ulong.MaxValue, 125);
        Command.Parameters.Add(new DuckDBParameter(value));
        Command.ExecuteNonQuery();

        Command.CommandText = "SELECT * from HugeIntTests;";

        using var reader = Command.ExecuteReader();
        reader.Read();

        var receivedValue = reader.GetFieldValue<BigInteger>(1);
        receivedValue.Should().Be(value);
    }

    [Fact]
    public void SimpleNegativeHugeIntTest()
    {
        Command.CommandText = $"SELECT {DuckDBHugeInt.HugeIntMinValue}::HUGEINT;";
        Command.ExecuteNonQuery();

        var scalar = Command.ExecuteScalar();
        scalar.Should().Be(DuckDBHugeInt.HugeIntMinValue);

        using var reader = Command.ExecuteReader();
        reader.Read();
        var receivedValue = reader.GetFieldValue<BigInteger>(0);
        receivedValue.Should().Be(DuckDBHugeInt.HugeIntMinValue);
    }

    [Fact]
    public void BindNegativeHugeIntValueTest()
    {
        Command.CommandText = "CREATE TABLE NegativeHugeIntTests (key INTEGER, value HugeInt)";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO NegativeHugeIntTests VALUES (9, ?);";

        var value = DuckDBHugeInt.HugeIntMinValue;
        Command.Parameters.Add(new DuckDBParameter(value));
        Command.ExecuteNonQuery();

        Command.CommandText = "SELECT * from NegativeHugeIntTests;";

        using var reader = Command.ExecuteReader();
        reader.Read();

        var receivedValue = reader.GetFieldValue<BigInteger>(1);
        receivedValue.Should().Be(value);
    }
}