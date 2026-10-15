using System.Globalization;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Data.Mapping;

namespace DuckDB.NET.Test;

// DuckDB.NET works out the integer that a DECIMAL column stores for a decimal with its own arithmetic, and DuckDB
// does not check a chunk that is appended. These tests guard that arithmetic at its edges: the last value a column
// has room for and the first it has not, in each storage size and on both paths of DecimalVectorDataWriter.
// A stored value is compared with DuckDB's own cast of the same digits, not read back: a round trip through the
// reader does not show a mistake that the reader makes too.
public class DecimalWriteConversionTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Theory]
    // The largest value of a column, in each storage size: 16, 32 and 64 bits.
    [InlineData("DECIMAL(4,0)", "9999")]
    [InlineData("DECIMAL(4,1)", "999.9")]
    [InlineData("DECIMAL(9,2)", "9999999.99")]
    [InlineData("DECIMAL(18,4)", "99999999999999.9999")]
    // 128 bits. An integer of 64 bits whose product with the power of ten needs the upper 64.
    [InlineData("DECIMAL(38,18)", "18446744073709551615")]
    [InlineData("DECIMAL(37,18)", "9999999999999999999")]
    // The lower 64 bits of the product are zero, so the negative value needs the carry into the upper 64.
    [InlineData("DECIMAL(38,19)", "35184372088832")]
    [InlineData("DECIMAL(38,0)", "18446744073709551616")]
    // An integer longer than 64 bits.
    [InlineData("DECIMAL(38,10)", "9999999999999999999999999999")]
    // Digits that the column has no room for after the point are dropped.
    [InlineData("DECIMAL(38,10)", "0.1428571428571428571428571428", "0.1428571428")]
    [InlineData("DECIMAL(18,4)", "99999999999999.99999999999999", "99999999999999.9999")]
    // The largest decimal, in columns with more room in front of the point than a decimal has digits.
    [InlineData("DECIMAL(38,0)", "79228162514264337593543950335")]
    [InlineData("DECIMAL(38,9)", "79228162514264337593543950335")]
    // A short value whose scale is 20 or more away from the scale of the column.
    [InlineData("DECIMAL(38,38)", "0.5")]
    [InlineData("DECIMAL(38,25)", "1")]
    // Exactly 20 away, in both directions: the first that the 64-bit path does not take.
    [InlineData("DECIMAL(38,20)", "1")]
    [InlineData("DECIMAL(4,0)", "0.00000000000000000001", "0")]
    [InlineData("DECIMAL(38,25)", "9999999999999")]
    [InlineData("DECIMAL(4,0)", "0.0000000000000000000000000001", "0")]
    public void AValueThatFitsIsStoredAsDuckDBCastsIt(string columnType, string digits, string stored = null)
    {
        foreach (var text in new[] { digits, "-" + digits })
        {
            Connection.ExecuteNonQuery($"CREATE OR REPLACE TABLE decimalWriteStored (value {columnType})");

            using (var appender = Connection.CreateAppender("decimalWriteStored"))
            {
                appender.CreateRow().AppendValue(decimal.Parse(text, CultureInfo.InvariantCulture)).EndRow();
            }

            var expected = text.Replace(digits, stored ?? digits);

            Command.CommandText = $"SELECT value = '{expected}'::{columnType} FROM decimalWriteStored";
            Command.ExecuteScalar().Should().Be(true, $"{text} in a {columnType} column");
        }
    }

    [Theory]
    // The first value with a digit too many, in each storage size.
    [InlineData("DECIMAL(4,0)", "10000")]
    [InlineData("DECIMAL(4,1)", "1000")]
    [InlineData("DECIMAL(9,2)", "10000000.00")]
    [InlineData("DECIMAL(18,4)", "100000000000000")]
    // It fits the 64 bits of the column, as every value of 19 digits does.
    [InlineData("DECIMAL(18,0)", "1000000000000000000")]
    [InlineData("DECIMAL(18,0)", "9223372036854775807")]
    // 128 bits, from an integer of 64 bits.
    [InlineData("DECIMAL(37,18)", "18446744073709551615")]
    [InlineData("DECIMAL(37,18)", "10000000000000000000")]
    // An integer longer than 64 bits.
    [InlineData("DECIMAL(38,18)", "100000000000000000000")]
    [InlineData("DECIMAL(38,10)", "10000000000000000000000000000")]
    [InlineData("DECIMAL(38,10)", "79228162514264337593543950335")]
    [InlineData("DECIMAL(18,4)", "100000000000000.00000000000001")]
    // A short value whose scale is 20 or more away from the scale of the column.
    [InlineData("DECIMAL(38,38)", "1")]
    [InlineData("DECIMAL(38,25)", "10000000000000")]
    public void AValueWithMoreDigitsThanItsColumnIsRefused(string columnType, string digits)
    {
        foreach (var text in new[] { digits, "-" + digits })
        {
            Connection.ExecuteNonQuery($"CREATE OR REPLACE TABLE decimalWriteRefused (value {columnType})");

            using (var appender = Connection.CreateAppender("decimalWriteRefused"))
            {
                appender.CreateRow().AppendValue(0m).EndRow();

                var row = appender.CreateRow();
                row.Invoking(r => r.AppendValue(decimal.Parse(text, CultureInfo.InvariantCulture)))
                    .Should().Throw<OverflowException>().WithMessage($"Value {text} has too many digits for a {columnType} column");

                // The row was left unfinished, so the appender takes no more rows.
                appender.Invoking(a => a.CreateRow()).Should().Throw<InvalidOperationException>();
            }

            // The row before it is kept.
            Command.CommandText = "SELECT count(*) FROM decimalWriteRefused WHERE value = 0";
            Command.ExecuteScalar().Should().Be(1L, $"{text} in a {columnType} column");
        }
    }

    [Fact]
    public void NegativeZeroIsStoredAsZero()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE decimalWriteZero (a DECIMAL(4,1), b DECIMAL(9,2), c DECIMAL(18,4), d DECIMAL(38,10), e DECIMAL(38,30))");

        var negativeZero = new decimal(0, 0, 0, true, 2);

        using (var appender = Connection.CreateAppender("decimalWriteZero"))
        {
            appender.CreateRow().AppendValue(negativeZero).AppendValue(negativeZero).AppendValue(negativeZero).AppendValue(negativeZero).AppendValue(negativeZero).EndRow();
        }

        Command.CommandText = "SELECT a = 0 AND b = 0 AND c = 0 AND d = 0 AND e = 0 FROM decimalWriteZero";
        Command.ExecuteScalar().Should().Be(true);
    }

    [Fact]
    public void AListItemWithMoreDigitsThanItsColumnIsRefused()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE decimalWriteList (value DECIMAL(4,1)[])");

        using (var appender = Connection.CreateAppender("decimalWriteList"))
        {
            appender.CreateRow().AppendValue(new List<decimal> { 999.9m, -999.9m }).EndRow();

            var row = appender.CreateRow();
            row.Invoking(r => r.AppendValue(new List<decimal> { 1.5m, 1000m }))
                .Should().Throw<OverflowException>().WithMessage("Value 1000 has too many digits for a DECIMAL(4,1) column");
        }

        Command.CommandText = "SELECT value::VARCHAR FROM decimalWriteList";
        Command.ExecuteScalar().Should().Be("[999.9, -999.9]");
    }

    [Fact]
    public void AMappedValueWithMoreDigitsThanItsColumnIsRefused()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE decimalWriteMapped (price DECIMAL(9,2))");

        using (var appender = Connection.CreateAppender<Product, ProductMap>("decimalWriteMapped"))
        {
            appender.AppendRecords([new Product { Price = 9999999.99m }]);

            appender.Invoking(a => a.AppendRecords([new Product { Price = 10000000m }]))
                .Should().Throw<OverflowException>().WithMessage("Value 10000000 has too many digits for a DECIMAL(9,2) column");
        }

        Command.CommandText = "SELECT price::VARCHAR FROM decimalWriteMapped";
        Command.ExecuteScalar().Should().Be("9999999.99");
    }

    // The result of a function is a DECIMAL(38,18): 20 digits in front of the point.
    [Fact]
    public void AScalarFunctionResultWithMoreDigitsThanItsTypeIsRefused()
    {
        Connection.RegisterScalarFunction<int, decimal>("decimal_write_scalar", value => value == 0 ? 99999999999999999999m : 100000000000000000000m);

        Command.CommandText = "SELECT decimal_write_scalar(0) = 99999999999999999999";
        Command.ExecuteScalar().Should().Be(true);

        Command.CommandText = "SELECT decimal_write_scalar(1)";
        Command.Invoking(command => command.ExecuteScalar())
            .Should().Throw<DuckDBException>().WithMessage("*Value 100000000000000000000 has too many digits for a DECIMAL(38,18) column*");
    }

    [Fact]
    public void ATableFunctionValueWithMoreDigitsThanItsTypeIsRefused()
    {
        Connection.RegisterTableFunction("decimal_write_table",
            (int count) => Enumerable.Range(0, count).Select(index => index == 0 ? 99999999999999999999m : 100000000000000000000m),
            value => new { Value = value });

        Command.CommandText = "SELECT Value = 99999999999999999999 FROM decimal_write_table(1)";
        Command.ExecuteScalar().Should().Be(true);

        Command.CommandText = "SELECT * FROM decimal_write_table(2)";
        Command.Invoking(command => command.ExecuteScalar())
            .Should().Throw<DuckDBException>().WithMessage("*Value 100000000000000000000 has too many digits for a DECIMAL(38,18) column*");
    }

    public class Product
    {
        public decimal Price { get; set; }
    }

    public class ProductMap : DuckDBAppenderMap<Product>
    {
        public ProductMap()
        {
            Map(product => product.Price);
        }
    }
}
