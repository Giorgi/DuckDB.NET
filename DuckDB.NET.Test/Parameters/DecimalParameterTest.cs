using System;
using System.Globalization;
using System.Text.RegularExpressions;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class DecimalParameterTests
{
    [Fact]
    public void SimpleTest()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var values = new[]{0m, decimal.Zero, decimal.MinValue,
            decimal.MaxValue, decimal.MaxValue / 3, decimal.One,
            decimal.One / 2, decimal.One / 3, decimal.MinusOne,
            //decimal.MinusOne / 2, 
            decimal.MinusOne / 3};

        foreach (var value in values)
        {
            var command = connection.CreateCommand();
            command.CommandText = $"SELECT {Convert.ToString(value, CultureInfo.InvariantCulture)};";
            command.ExecuteNonQuery();

            //var scalar = command.ExecuteScalar();
            //scalar.Should().Be(value);

            var reader = command.ExecuteReader();
            reader.Read();
            var receivedValue = reader.GetDecimal(0);
            receivedValue.Should().Be(value);
        }
    }

    [Fact]
    public void InsertSelectValueTest()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var command = connection.CreateCommand();
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

        void DecimalTests(decimal[] values, int precision, int scale)
        {
            command.CommandText = $"CREATE TABLE DecimalValuesTests (key INTEGER, value decimal({precision}, {scale}))";
            command.ExecuteNonQuery();

            foreach (var value in values)
            {
                command.CommandText = "Insert Into DecimalValuesTests (key, value) values (1, ?)";
                command.Parameters.Add(new DuckDBParameter(value));
                command.ExecuteNonQuery();

                command.Parameters.Clear();
                command.CommandText = "SELECT value from DecimalValuesTests;";

                var scalar = command.ExecuteScalar();
                scalar.Should().Be(value);

                var reader = command.ExecuteReader();
                reader.Read();

                var receivedValue = reader.GetDecimal(0);
                receivedValue.Should().Be(value);

                reader.GetFieldType(0).Should().Be(typeof(decimal));

                command.CommandText = "Delete from DecimalValuesTests";
                command.ExecuteNonQuery();
            }

            command.CommandText = "Drop TABLE DecimalValuesTests";
            command.ExecuteNonQuery();
        }
    }

    [Fact]
    public void InsertSelectValueTestWithCulture()
    {
        var defaultCulture = System.Threading.Thread.CurrentThread.CurrentCulture;
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var command = connection.CreateCommand();
        DecimalTests(new[] { "fr-fr", "en-us" }, decimal.One / 2, 38, 15);

        void DecimalTests(string[] cultures, decimal value, int precision, int scale)
        {
            command.CommandText = $"CREATE TABLE DecimalValuesTests (key INTEGER, value decimal({precision}, {scale}))";
            command.ExecuteNonQuery();

            foreach (var culture in cultures)
            {
                System.Threading.Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo(culture);
                command.CommandText = "Insert Into DecimalValuesTests (key, value) values (1, ?)";
                command.Parameters.Add(new DuckDBParameter(value));
                command.ExecuteNonQuery();

                command.Parameters.Clear();
                command.CommandText = "SELECT value from DecimalValuesTests;";

                var scalar = command.ExecuteScalar();
                scalar.Should().Be(value);

                var reader = command.ExecuteReader();
                reader.Read();

                var receivedValue = reader.GetDecimal(0);
                receivedValue.Should().Be(value);

                reader.GetFieldType(0).Should().Be(typeof(decimal));

                command.CommandText = "Delete from DecimalValuesTests";
                command.ExecuteNonQuery();
            }

            command.CommandText = "Drop TABLE DecimalValuesTests";
            command.ExecuteNonQuery();
            System.Threading.Thread.CurrentThread.CurrentCulture = defaultCulture;
        }
    }
}