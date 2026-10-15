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
        // The parentheses matter: a cast binds tighter than the minus sign, and 2^127 itself is not a HUGEINT.
        Command.CommandText = $"SELECT ({DuckDBHugeInt.HugeIntMinValue})::HUGEINT;";
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
    public void HugeIntMinimumAndMaximumRoundTrip()
    {
        // HUGEINT is a 128-bit two's complement integer: -2^127 to 2^127 - 1.
        DuckDBHugeInt.HugeIntMinValue.Should().Be(-BigInteger.Pow(2, 127));
        DuckDBHugeInt.HugeIntMaxValue.Should().Be(BigInteger.Pow(2, 127) - 1);

        Command.CommandText = "CREATE OR REPLACE TABLE HugeIntRangeTests (value HUGEINT);";
        Command.ExecuteNonQuery();

        BigInteger? minimum = DuckDBHugeInt.HugeIntMinValue;
        BigInteger? maximum = DuckDBHugeInt.HugeIntMaxValue;

        // Once as parameters and once through the appender.
        Command.CommandText = "INSERT INTO HugeIntRangeTests VALUES ($1), ($2);";
        Command.Parameters.Add(new DuckDBParameter(minimum.Value));
        Command.Parameters.Add(new DuckDBParameter(maximum.Value));
        Command.ExecuteNonQuery();
        Command.Parameters.Clear();

        using (var appender = Connection.CreateAppender("HugeIntRangeTests"))
        {
            appender.CreateRow().AppendValue(minimum).EndRow();
            appender.CreateRow().AppendValue(maximum).EndRow();
        }

        Command.CommandText = "SELECT value::VARCHAR FROM HugeIntRangeTests ORDER BY rowid;";
        using var reader = Command.ExecuteReader();

        foreach (var expected in new[] { minimum.Value, maximum.Value, minimum.Value, maximum.Value })
        {
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be(expected.ToString());
        }

        // One step outside the range on either side is rejected.
        FluentActions.Invoking(() => new DuckDBHugeInt(minimum.Value - 1)).Should().Throw<ArgumentOutOfRangeException>();
        FluentActions.Invoking(() => new DuckDBHugeInt(maximum.Value + 1)).Should().Throw<ArgumentOutOfRangeException>();
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
    // 2^127 + 42 and 2^128 - 1 fit UHUGEINT but not HUGEINT.
    public static IEnumerable<object[]> UnsignedHugeIntValues() =>
    [
        [BigInteger.Zero],
        [BigInteger.Pow(2, 127) + 42],
        [BigInteger.Pow(2, 128) - 1],
    ];

    [Theory]
    [MemberData(nameof(UnsignedHugeIntValues))]
    public void BindBigIntegerToUnsignedHugeIntColumn(BigInteger value)
    {
        Command.CommandText = "CREATE OR REPLACE TABLE UHugeIntParameterTests (value UHUGEINT);";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO UHugeIntParameterTests VALUES ($1);";
        Command.Parameters.Add(new DuckDBParameter(value));
        Command.ExecuteNonQuery();

        // The parameter is compared with the UHUGEINT column, so DuckDB reports its type as UHUGEINT here too.
        Command.CommandText = "SELECT value::VARCHAR FROM UHugeIntParameterTests WHERE value = $1;";

        Command.ExecuteScalar().Should().Be(value.ToString());
    }

    [Fact]
    public void BindOutOfRangeBigIntegerToUnsignedHugeIntColumnThrows()
    {
        Command.CommandText = "CREATE OR REPLACE TABLE UHugeIntParameterRangeTests (value UHUGEINT);";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO UHugeIntParameterRangeTests VALUES ($1);";

        foreach (var value in new[] { BigInteger.MinusOne, BigInteger.Pow(2, 128) })
        {
            Command.Parameters.Clear();
            Command.Parameters.Add(new DuckDBParameter(value));

            Command.Invoking(command => command.ExecuteNonQuery()).Should().Throw<ArgumentOutOfRangeException>();
        }
    }

    // With a cast or no context, DuckDB reports no parameter type and the value picks its own type.
    [Theory]
    [InlineData("SELECT ($1::UHUGEINT)::VARCHAR;")]
    [InlineData("SELECT $1::VARCHAR;")]
    public void BindBigIntegerAboveHugeIntRangeWithoutColumn(string query)
    {
        var value = BigInteger.Pow(2, 127) + 42;

        Command.CommandText = query;
        Command.Parameters.Add(new DuckDBParameter(value));

        Command.ExecuteScalar().Should().Be(value.ToString());
    }

    [Theory]
    [InlineData("170141183460469231731687303715884105727", "HUGEINT")]
    [InlineData("-170141183460469231731687303715884105728", "HUGEINT")]
    [InlineData("170141183460469231731687303715884105728", "UHUGEINT")]
    [InlineData("340282366920938463463374607431768211455", "UHUGEINT")]
    [InlineData("340282366920938463463374607431768211456", "BIGNUM")]
    [InlineData("-170141183460469231731687303715884105729", "BIGNUM")]
    public void BigIntegerWithoutColumnPicksTheSmallestTypeThatFits(string text, string expectedType)
    {
        var value = BigInteger.Parse(text);

        Command.CommandText = "SELECT typeof($1), $1::VARCHAR;";
        Command.Parameters.Add(new DuckDBParameter(value));

        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();

        reader.GetString(0).Should().Be(expectedType);
        reader.GetString(1).Should().Be(text);
    }
}