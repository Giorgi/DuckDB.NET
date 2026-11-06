namespace DuckDB.NET.Test.Parameters;

public class HugeIntParameterTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void SimpleTest()
    {
        Command.CommandText = "SELECT 125::HUGEINT;";
        Command.ExecuteNonQuery();

        var scalar = Command.ExecuteScalar();
        scalar.Should().Be((Int128)125);

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

        reader.GetFieldType(0).Should().Be(typeof(Int128));
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
        scalar.Should().Be(Int128.MinValue);

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

        result.Should().BeOfType<Int128>().Subject
              .Should().Be((Int128)value);
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

    // Int128 and UInt128 are sent as a HUGEINT and a UHUGEINT. They used to be sent as text.
    public static IEnumerable<object[]> Int128Values() =>
    [
        [Int128.MinValue, "-170141183460469231731687303715884105728"],
        [(Int128)long.MinValue - 1, "-9223372036854775809"],
        [Int128.NegativeOne, "-1"],
        [Int128.Zero, "0"],
        [(Int128)ulong.MaxValue + 1, "18446744073709551616"],
        [Int128.MaxValue, "170141183460469231731687303715884105727"],
    ];

    public static IEnumerable<object[]> UInt128Values() =>
    [
        [UInt128.Zero, "0"],
        [(UInt128)ulong.MaxValue + 1, "18446744073709551616"],
        [(UInt128)Int128.MaxValue + 1, "170141183460469231731687303715884105728"],
        [UInt128.MaxValue, "340282366920938463463374607431768211455"],
    ];

    [Theory]
    [MemberData(nameof(Int128Values))]
    public void Int128IsSentAsAHugeInt(Int128 value, string text)
    {
        var parameter = new DuckDBParameter(value);
        parameter.DbType.Should().Be(DbType.VarNumeric);

        Command.CommandText = "SELECT typeof($1), $1::VARCHAR, $1 = $2::HUGEINT;";
        Command.Parameters.Add(parameter);
        Command.Parameters.Add(new DuckDBParameter(text));

        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("HUGEINT");
        reader.GetString(1).Should().Be(text);
        reader.GetBoolean(2).Should().BeTrue();
    }

    [Theory]
    [MemberData(nameof(UInt128Values))]
    public void UInt128IsSentAsAUHugeInt(UInt128 value, string text)
    {
        var parameter = new DuckDBParameter(value);
        parameter.DbType.Should().Be(DbType.VarNumeric);

        Command.CommandText = "SELECT typeof($1), $1::VARCHAR, $1 = $2::UHUGEINT;";
        Command.Parameters.Add(parameter);
        Command.Parameters.Add(new DuckDBParameter(text));

        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("UHUGEINT");
        reader.GetString(1).Should().Be(text);
        reader.GetBoolean(2).Should().BeTrue();
    }

    // A column of another type: DuckDB casts the value, and refuses one that does not fit.
    [Fact]
    public void Int128AndUInt128AreCastToTheTypeOfTheColumn()
    {
        Command.CommandText = "CREATE OR REPLACE TABLE Int128ParameterTargets (signed HUGEINT, unsigned UHUGEINT, i INTEGER, d DECIMAL(38,0), v VARCHAR);";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO Int128ParameterTargets VALUES ($1, $2, $3, $4, $5);";
        Command.Parameters.Add(new DuckDBParameter((UInt128)Int128.MaxValue));
        Command.Parameters.Add(new DuckDBParameter(Int128.Zero));
        Command.Parameters.Add(new DuckDBParameter((Int128)(-42)));
        Command.Parameters.Add(new DuckDBParameter(Int128.Parse("-99999999999999999999999999999999999999")));
        Command.Parameters.Add(new DuckDBParameter(UInt128.MaxValue));
        Command.ExecuteNonQuery();

        Command.CommandText = "SELECT signed::VARCHAR, unsigned::VARCHAR, i, d::VARCHAR, v FROM Int128ParameterTargets;";
        using (var reader = Command.ExecuteReader())
        {
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("170141183460469231731687303715884105727");
            reader.GetString(1).Should().Be("0");
            reader.GetInt32(2).Should().Be(-42);
            reader.GetString(3).Should().Be("-99999999999999999999999999999999999999");
            reader.GetString(4).Should().Be("340282366920938463463374607431768211455");
        }

        foreach (var (column, value) in new (string, object)[]
                 {
                     ("signed", (UInt128)Int128.MaxValue + 1), ("unsigned", Int128.NegativeOne), ("i", (Int128)int.MaxValue + 1),
                 })
        {
            Command.CommandText = $"INSERT INTO Int128ParameterTargets ({column}) VALUES ($1);";
            Command.Parameters.Clear();
            Command.Parameters.Add(new DuckDBParameter(value));
            Command.Invoking(command => command.ExecuteNonQuery()).Should().Throw<DuckDBException>($"{value} into {column}");
        }
    }

    [Fact]
    public void NullInt128IsSentAsNull()
    {
        Command.CommandText = "SELECT $1 IS NULL;";
        Command.Parameters.Add(new DuckDBParameter((Int128?)null));
        Command.ExecuteScalar().Should().Be(true);
    }

    [Fact]
    public void ListsOfInt128AndUInt128()
    {
        Command.CommandText = "CREATE OR REPLACE TABLE Int128ListParameters (signed HUGEINT[], unsigned UHUGEINT[]);";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO Int128ListParameters VALUES ($1, $2);";
        Command.Parameters.Add(new DuckDBParameter(new List<Int128> { Int128.MinValue, Int128.MaxValue }));
        Command.Parameters.Add(new DuckDBParameter(new UInt128?[] { UInt128.MaxValue, null }));
        Command.ExecuteNonQuery();

        Command.CommandText = """
            SELECT signed = ['-170141183460469231731687303715884105728', '170141183460469231731687303715884105727']::HUGEINT[],
                   unsigned IS NOT DISTINCT FROM ['340282366920938463463374607431768211455', NULL]::UHUGEINT[]
            FROM Int128ListParameters;
            """;
        Command.Parameters.Clear();
        using (var reader = Command.ExecuteReader())
        {
            reader.Read().Should().BeTrue();
            reader.GetBoolean(0).Should().BeTrue();
            reader.GetBoolean(1).Should().BeTrue();
        }

        // With no column, the list takes the type of its items.
        Command.CommandText = "SELECT typeof($1), $1::VARCHAR, typeof($2), $2::VARCHAR;";
        Command.Parameters.Add(new DuckDBParameter(new[] { Int128.MinValue, Int128.One }));
        Command.Parameters.Add(new DuckDBParameter(new List<UInt128> { UInt128.MaxValue }));
        using (var reader = Command.ExecuteReader())
        {
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be("HUGEINT[]");
            reader.GetString(1).Should().Be("[-170141183460469231731687303715884105728, 1]");
            reader.GetString(2).Should().Be("UHUGEINT[]");
            reader.GetString(3).Should().Be("[340282366920938463463374607431768211455]");
        }
    }
}