using System;
using System.Globalization;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

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


        values = new[] { decimal.One / 3, decimal.MinusOne / 3 };

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
        DecimalTests(new[]
        {
            0m, decimal.Zero,
            decimal.One,
            decimal.One / 2, decimal.MinusOne,
            decimal.MinusOne / 2
        }, 38, 15);

        DecimalTests(new[]
        {
            decimal.MinValue, decimal.MaxValue
        }, 38, 0);

        DecimalTests(new[]
        {
            decimal.One/3, decimal.MinusOne/3
        }, 38, 28);

        DecimalTests(new[]
        {
            0.3333M, 56.1234M
        }, 8, 4);

        DecimalTests(new[]
        {
            0.33M, 12.34M
        }, 4, 2);

        void DecimalTests(decimal[] values, int precision, int scale)
        {
            Command.CommandText = $"CREATE TABLE DecimalValuesTests (key INTEGER, value decimal({precision}, {scale}))";
            Command.ExecuteNonQuery();

            foreach (var value in values)
            {
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
        }
    }

    [Fact]
    public void InsertSelectValueTestWithCulture()
    {
        var defaultCulture = System.Threading.Thread.CurrentThread.CurrentCulture;

        DecimalTests(new[] { "fr-fr", "en-us" }, decimal.One / 2, 38, 15);

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
}