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

        var reader = Command.ExecuteReader();
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

        var reader = Command.ExecuteReader();
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

        var reader = Command.ExecuteReader();
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

        var reader = Command.ExecuteReader();
        reader.Read();

        var receivedValue = reader.GetFieldValue<BigInteger>(1);
        receivedValue.Should().Be(value);
    }

    [Fact]
    public void BindParameterWithoutTable_HugeInt()
    {
        // Generate a value larger than long.MaxValue to ensure it is treated as HUGEINT
        var value = new BigInteger(ulong.MaxValue) + Faker.Random.Int(1, 10_000);

        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));

        var result = Command.ExecuteScalar();

        result.Should().BeOfType<BigInteger>().Subject
              .Should().Be(value);
    }
}