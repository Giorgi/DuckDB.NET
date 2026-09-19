using System.Globalization;

namespace DuckDB.NET.Test.Parameters;

public class DecimalParameterTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void SimpleTest()
    {
        var values = new[]{0m, decimal.Zero, decimal.MinValue,
            decimal.MaxValue, decimal.MaxValue / 3, decimal.One,
            decimal.One / 2,  decimal.MinusOne,
            decimal.MinusOne / 2};

        foreach (var value in values)
        {
            Command.CommandText = $"SELECT {Convert.ToString(value, CultureInfo.InvariantCulture)}::DECIMAL(38,9);";
            Command.ExecuteNonQuery();

            var scalar = Command.ExecuteScalar();
            scalar.Should().Be(value);

            var reader = Command.ExecuteReader();
            reader.Read();
            var receivedValue = reader.GetDecimal(0);
            receivedValue.Should().Be(value);
        }


        values = [decimal.One / 3, decimal.MinusOne / 3];

        foreach (var value in values)
        {
            Command.CommandText = $"SELECT {Convert.ToString(value, CultureInfo.InvariantCulture)}::DECIMAL(38,28);";
            Command.ExecuteNonQuery();

            var scalar = Command.ExecuteScalar();
            scalar.Should().Be(value);

            var reader = Command.ExecuteReader();
            reader.Read();
            var receivedValue = reader.GetDecimal(0);
            receivedValue.Should().Be(value);
        }
    }

    [Fact]
    public void InsertSelectValueTest()
    {
        DecimalTests([
            0m, decimal.Zero,
            decimal.One,
            decimal.One / 2, decimal.MinusOne,
            decimal.MinusOne / 2
        ], 38, 15);

        DecimalTests([
            decimal.MinValue, decimal.MaxValue
        ], 38, 0);

        DecimalTests([
            decimal.One/3, decimal.MinusOne/3, -123456789.987654321m
        ], 38, 28);

        DecimalTests([
            0.3333M, 56.1234M
        ], 8, 4);

        DecimalTests([
            0.33M, 12.34M
        ], 4, 2);

        void DecimalTests(decimal[] values, int precision, int scale)
        {
            Command.CommandText = $"CREATE TABLE DecimalValuesTests (key INTEGER, value decimal({precision}, {scale}))";
            Command.ExecuteNonQuery();

            foreach (var value in values)
            {
                Command.CommandText = $"Insert Into DecimalValuesTests (key, value) values (1, ?::decimal({precision}, {scale}))";
                Command.Parameters.Add(new DuckDBParameter(value));
                Command.ExecuteNonQuery();

                Command.Parameters.Clear();
                Command.CommandText = "SELECT value from DecimalValuesTests;";

                var scalar = Command.ExecuteScalar();
                scalar.Should().Be(value);

                var reader = Command.ExecuteReader();
                reader.Read();

                var receivedValue = reader.GetDecimal(0);
                receivedValue.Should().Be(value);

                reader.GetFieldType(0).Should().Be(typeof(decimal));

                Command.CommandText = "Delete from DecimalValuesTests";
                Command.ExecuteNonQuery();
            }

            Command.CommandText = "Drop TABLE DecimalValuesTests";
            Command.ExecuteNonQuery();
        }
    }

    [Fact]
    public void InsertSelectValueTestWithCulture()
    {
        var defaultCulture = System.Threading.Thread.CurrentThread.CurrentCulture;

        DecimalTests(["fr-fr", "en-us"], decimal.One / 2, 38, 15);

        void DecimalTests(string[] cultures, decimal value, int precision, int scale)
        {
            Command.CommandText = $"CREATE TABLE DecimalValuesTests (key INTEGER, value decimal({precision}, {scale}))";
            Command.ExecuteNonQuery();

            foreach (var culture in cultures)
            {
                System.Threading.Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo(culture);
                Command.CommandText = "Insert Into DecimalValuesTests (key, value) values (1, ?)";
                Command.Parameters.Add(new DuckDBParameter(value));
                Command.ExecuteNonQuery();

                Command.Parameters.Clear();
                Command.CommandText = "SELECT value from DecimalValuesTests;";

                var scalar = Command.ExecuteScalar();
                scalar.Should().Be(value);

                var reader = Command.ExecuteReader();
                reader.Read();

                var receivedValue = reader.GetDecimal(0);
                receivedValue.Should().Be(value);

                reader.GetFieldType(0).Should().Be(typeof(decimal));

                Command.CommandText = "Delete from DecimalValuesTests";
                Command.ExecuteNonQuery();
            }

            Command.CommandText = "Drop TABLE DecimalValuesTests";
            Command.ExecuteNonQuery();
            System.Threading.Thread.CurrentThread.CurrentCulture = defaultCulture;
        }
    }

    [Fact]
    public void BindParameterWithoutTable()
    {
        decimal[] values = [decimal.Zero, 0.00m, 123456789.987654321m, -123456789.987654321m, 1.230m, -1.23m,
                            0.000000001m, -0.000000001m, 1000000.000000001m, -1000000.000000001m, 1.123456789012345678901m];

        foreach (var value in values)
        {
            Command.CommandText = "SELECT ?;";
            Command.Parameters.Clear();
            Command.Parameters.Add(new DuckDBParameter(value));

            var result = Command.ExecuteScalar();

            result.Should().BeOfType<decimal>().Subject.Should().Be(value);
        }
    }

    [Theory]
    [InlineData("SELECT 1.5::DECIMAL(38, 30)", 1.5)]
    [InlineData("SELECT -1.5::DECIMAL(38, 30)", -1.5)]
    [InlineData("SELECT 0.0::DECIMAL(38, 30)", 0.0)]
    [InlineData("SELECT 123.456::DECIMAL(38, 30)", 123.456)]
    public void ReadHighScaleDecimal(string query, double expected)
    {
        // DuckDB DECIMAL(38, 30) uses HugeInt internal type with scale 30,
        // which exceeds .NET decimal's max power of 10^28.
        Command.CommandText = query;

        using var reader = Command.ExecuteReader();
        reader.Read();

        var value = reader.GetDecimal(0);
        value.Should().Be((decimal)expected);
    }

    [Theory]
    [InlineData("SELECT 12345678901234567890.12::DECIMAL(38, 2)", "12345678901234567890.12")]
    [InlineData("SELECT -99999999999999999999999999.999::DECIMAL(38, 3)", "-99999999999999999999999999.999")]
    [InlineData("SELECT 0.01::DECIMAL(38, 2)", "0.01")]
    public void ReadWideDecimalLowScale(string query, string expected)
    {
        // DuckDB DECIMAL(38, 2) uses HugeInt internal type (width > 18)
        // but with a low scale that fits comfortably in .NET decimal.
        Command.CommandText = query;

        using var reader = Command.ExecuteReader();
        reader.Read();

        var value = reader.GetDecimal(0);
        value.Should().Be(decimal.Parse(expected, CultureInfo.InvariantCulture));
    }

    [Theory]
    // Scale 27: below MaxDecimalScale, HugeInt path, full precision
    [InlineData("SELECT 1.234567890123456789012345678::DECIMAL(38, 27)", "1.234567890123456789012345678")]
    [InlineData("SELECT -1.234567890123456789012345678::DECIMAL(38, 27)", "-1.234567890123456789012345678")]
    // Scale 28: exactly at MaxDecimalScale, HugeInt path, full precision
    [InlineData("SELECT 1.2345678901234567890123456789::DECIMAL(38, 28)", "1.2345678901234567890123456789")]
    [InlineData("SELECT -1.2345678901234567890123456789::DECIMAL(38, 28)", "-1.2345678901234567890123456789")]
    // Scale 29: above MaxDecimalScale — 29th fractional digit is truncated (not rounded)
    [InlineData("SELECT 1.23456789012345678901234567891::DECIMAL(38, 29)", "1.2345678901234567890123456789")]
    [InlineData("SELECT -1.23456789012345678901234567891::DECIMAL(38, 29)", "-1.2345678901234567890123456789")]
    [InlineData("SELECT 0.00000000000000000000000000019::DECIMAL(38, 29)", "0.0000000000000000000000000001")]
    [InlineData("SELECT 0.00000000000000000000000000009::DECIMAL(38, 29)", "0")]
    public void ReadDecimalAtScaleBoundary(string query, string expected)
    {
        Command.CommandText = query;

        using var reader = Command.ExecuteReader();
        reader.Read();

        var value = reader.GetDecimal(0);
        value.Should().Be(decimal.Parse(expected, CultureInfo.InvariantCulture));
    }

    [Fact]
    public void BindParameterInComparison()
    {
        var testCases = new (decimal value, bool expectedResult)[]
        {
            (decimal.Zero, true),
            (0.00m, true),
            (123456789.987654321m, false),
            (-123456789.987654321m, true),
            (1.230m, false),
            (-1.23m, true),
            (0.000000001m, true),
            (-0.000000001m, true),
            (1000000.000000001m, false),
            (-1000000.000000001m, true),
            (1.123456789012345678901m, false)
        };

        foreach (var (value, expectedResult) in testCases)
        {
            Command.CommandText = "SELECT 0.1 > ?;";
            Command.Parameters.Clear();
            Command.Parameters.Add(new DuckDBParameter(value));

            var result = Command.ExecuteScalar();

            result.Should().BeOfType<bool>().Subject.Should().Be(expectedResult);
        }
    }

    [Theory]
    // One case per internal storage type: SmallInt, Integer, BigInt, HugeInt
    [InlineData("SELECT 1234::DECIMAL(4, 0)", "1234")]
    [InlineData("SELECT -123456789::DECIMAL(9, 0)", "-123456789")]
    [InlineData("SELECT 123456789012345678::DECIMAL(18, 0)", "123456789012345678")]
    [InlineData("SELECT '99999999999999999999999999999999999999'::DECIMAL(38, 0)", "99999999999999999999999999999999999999")]
    [InlineData("SELECT '-99999999999999999999999999999999999999'::DECIMAL(38, 0)", "-99999999999999999999999999999999999999")]
    // Scale > 0 converts when the fractional part is zero
    [InlineData("SELECT 123.00::DECIMAL(38, 2)", "123")]
    [InlineData("SELECT -42.000::DECIMAL(9, 3)", "-42")]
    [InlineData("SELECT '0'::DECIMAL(38, 38)", "0")]
    public void ReadDecimalAsBigInteger(string query, string expected)
    {
        Command.CommandText = query;

        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<BigInteger>(0).Should().Be(BigInteger.Parse(expected));
        reader.GetFieldValue<BigInteger?>(0).Should().Be(BigInteger.Parse(expected));
    }

    [Theory]
    [InlineData("SELECT 123.45::DECIMAL(38, 2)")]
    [InlineData("SELECT 1.5::DECIMAL(4, 2)")]
    public void ReadDecimalWithFractionalPartAsBigIntegerThrows(string query)
    {
        Command.CommandText = query;

        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetFieldValue<BigInteger>(0)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void ReadWideDecimalAsDecimalThrowsOverflow()
    {
        Command.CommandText = "SELECT '99999999999999999999999999999999999999'::DECIMAL(38, 0)";

        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetDecimal(0)).Should().Throw<OverflowException>();
        reader.Invoking(r => r.GetValue(0)).Should().Throw<OverflowException>();

        reader.GetFieldValue<BigInteger>(0).Should().Be(BigInteger.Parse("99999999999999999999999999999999999999"));
    }

    [Theory]
    // One case per internal storage type: SmallInt, Integer, BigInt, HugeInt
    [InlineData("SELECT 9.9::DECIMAL(2, 1)", 2, 1, "99")]
    [InlineData("SELECT -12345.6789::DECIMAL(9, 4)", 9, 4, "-123456789")]
    [InlineData("SELECT 123456789012.345678::DECIMAL(18, 6)", 18, 6, "123456789012345678")]
    [InlineData("SELECT -123456789012.345678::DECIMAL(18, 6)", 18, 6, "-123456789012345678")]
    [InlineData("SELECT '-99999999999999999999999999999999999.999'::DECIMAL(38, 3)", 38, 3, "-99999999999999999999999999999999999999")]
    // Scale above decimal's 28-digit limit (bigIntRemainderShift regime)
    [InlineData("SELECT '-0.99999999999999999999999999999999999999'::DECIMAL(38, 38)", 38, 38, "-99999999999999999999999999999999999999")]
    public void ReadDecimalAsDuckDBDecimal(string query, int width, int scale, string unscaledValue)
    {
        Command.CommandText = query;

        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetProviderSpecificFieldType(0).Should().Be(typeof(DuckDBDecimal));

        var value = reader.GetFieldValue<DuckDBDecimal>(0);
        value.Width.Should().Be((byte)width);
        value.Scale.Should().Be((byte)scale);
        value.Value.ToBigInteger().Should().Be(BigInteger.Parse(unscaledValue));

        reader.GetProviderSpecificValue(0).Should().Be(value);
    }

    [Fact]
    public void ReadDecimalJustAboveDecimalMaxValueThrowsOverflow()
    {
        // Quotient is exactly decimal.MaxValue; adding the 0.5 fraction rounds up and overflows.
        Command.CommandText = "SELECT '79228162514264337593543950335.5'::DECIMAL(38, 1)";

        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetDecimal(0)).Should().Throw<OverflowException>();
        reader.GetFieldValue<DuckDBDecimal>(0).Value.ToBigInteger().Should().Be(BigInteger.Parse("792281625142643375935439503355"));
    }

    [Fact]
    public void ReadMapOfDecimalValues()
    {
        Command.CommandText = "SELECT MAP {'a': 1.5::DECIMAL(4, 2), 'b': 2.25::DECIMAL(4, 2)}";

        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetValue(0).Should().BeOfType<Dictionary<string, decimal>>()
              .Subject.Should().BeEquivalentTo(new Dictionary<string, decimal> { ["a"] = 1.5m, ["b"] = 2.25m });

        var providerValue = reader.GetProviderSpecificValue(0).Should().BeOfType<Dictionary<string, DuckDBDecimal>>().Subject;
        providerValue["a"].Scale.Should().Be(2);
        providerValue["a"].Value.ToBigInteger().Should().Be(150);
        providerValue["b"].Value.ToBigInteger().Should().Be(225);
    }

    [Fact]
    public void ReadListOfDecimalValues()
    {
        Command.CommandText = "SELECT [1.5::DECIMAL(4, 2), 2.25::DECIMAL(4, 2)]";

        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetValue(0).Should().BeOfType<List<decimal>>().Subject.Should().Equal(1.5m, 2.25m);

        var providerValue = reader.GetProviderSpecificValue(0).Should().BeOfType<List<DuckDBDecimal>>().Subject;
        providerValue.Select(v => v.Value.ToBigInteger()).Should().Equal(new BigInteger(150), new BigInteger(225));
    }

    [Fact]
    public void ReadNullDecimalAsBigIntegerAndDuckDBDecimal()
    {
        Command.CommandText = "SELECT NULL::DECIMAL(38, 0)";

        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<BigInteger?>(0).Should().BeNull();
        reader.GetFieldValue<DuckDBDecimal?>(0).Should().BeNull();
        reader.GetProviderSpecificValue(0).Should().Be(DBNull.Value);
    }
}
