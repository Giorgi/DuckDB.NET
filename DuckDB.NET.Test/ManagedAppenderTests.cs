using System;
using System.Globalization;
using DuckDB.NET.Data;
using Xunit;
using FluentAssertions;
using System.Collections.Generic;

namespace DuckDB.NET.Test;

public class DuckDBManagedAppenderTests
{
    [Fact]
    public void ManagedAppenderTests()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        using (var duckDbCommand = connection.CreateCommand())
        {
            var table = "CREATE TABLE managedAppenderTest(a BOOLEAN, b TINYINT, c SMALLINT, d INTEGER, e BIGINT, f UTINYINT, " +
                        "g USMALLINT, h UINTEGER, i UBIGINT, j REAL, k DOUBLE, l VARCHAR, m TIMESTAMP, n Date);";
            duckDbCommand.CommandText = table;
            duckDbCommand.ExecuteNonQuery();
        }

        var rows = 10;
        var date = DateTime.Today;
        using (var appender = connection.CreateAppender("managedAppenderTest"))
        {
            for (var i = 0; i < rows; i++)
            {
                var row = appender.CreateRow();
                row
                    .AppendValue(i % 2 == 0)
                    .AppendValue((sbyte?)i)
                    .AppendValue((short?)i)
                    .AppendValue((int?)i)
                    .AppendValue((long?)i)
                    .AppendValue((byte?)i)
                    .AppendValue((ushort?)i)
                    .AppendValue((uint?)i)
                    .AppendValue((ulong?)i)
                    .AppendValue((float)i)
                    .AppendValue((double)i)
                    .AppendValue($"{i}")
                    .AppendValue(date.AddDays(i))
                    .AppendNullValue()
                    .EndRow();
            }
        }

        using (var duckDbCommand = connection.CreateCommand())
        {
            duckDbCommand.CommandText = "SELECT * FROM managedAppenderTest";
            using var reader = duckDbCommand.ExecuteReader();

            var readRowIndex = 0;
            while (reader.Read())
            {
                var booleanCell = (bool)reader[0];
                var dateTimeCell = (DateTime)reader[12];

                booleanCell.Should().Be(readRowIndex % 2 == 0);
                dateTimeCell.Should().Be(date.AddDays(readRowIndex));

                for (int columnIndex = 1; columnIndex < 12; columnIndex++)
                {
                    var cell = (IConvertible)reader[columnIndex];
                    cell.ToInt32(CultureInfo.InvariantCulture).Should().Be(readRowIndex);
                }

                readRowIndex++;
            }
            readRowIndex.Should().Be(rows);
        }
    }

    [Fact]
    public void ManagedAppenderUnicodeTests()
    {
        var words = new List<string> { "hello", "안녕하세요", "Ø3mm CHAIN", null, "" };

        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        using (var duckDbCommand = connection.CreateCommand())
        {
            var table = "CREATE TABLE UnicodeAppenderTestTable (index INTEGER, words VARCHAR);";
            duckDbCommand.CommandText = table;
            duckDbCommand.ExecuteNonQuery();
        }

        using (var appender = connection.CreateAppender("UnicodeAppenderTestTable"))
        {
            for (int i = 0; i < words.Count; i++)
            {
                var row = appender.CreateRow();
                row.AppendValue(i).AppendValue(words[i]);

                row.EndRow();
            }

            appender.Close();
        }

        using (var duckDbCommand = connection.CreateCommand())
        {
            duckDbCommand.CommandText = "SELECT * FROM UnicodeAppenderTestTable";
            using var reader = duckDbCommand.ExecuteReader();

            var results = new List<string>();
            while (reader.Read())
            {
                var text = reader.IsDBNull(1) ? null : reader.GetString(1);
                results.Add(text);
            }

            results.Should().BeEquivalentTo(words);
        }
    }

    [Fact]
    public void IncompleteRowThrowsException()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        using (var duckDbCommand = connection.CreateCommand())
        {

            var table = "CREATE TABLE managedAppenderIncompleteTest(a BOOLEAN, b TINYINT, c SMALLINT, d INTEGER, e BIGINT, f UTINYINT, g USMALLINT, h UINTEGER, i UBIGINT, j REAL, k DOUBLE, l VARCHAR);";
            duckDbCommand.CommandText = table;
            duckDbCommand.ExecuteNonQuery();
        }

        connection.Invoking(dbConnection =>
        {
            using var appender = dbConnection.CreateAppender("managedAppenderIncompleteTest");
            var row = appender.CreateRow();
            row
                .AppendValue(true)
                .AppendValue((byte)1)
                .EndRow();
        }).Should().Throw<DuckDBException>();
    }

    [Fact]
    public void TableDoesNotExistsThrowsException()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        connection.Invoking(dbConnection =>
        {
            using var appender = dbConnection.CreateAppender("managedAppenderMissingTableTest");
            var row = appender.CreateRow();
            row
                .AppendValue(true)
                .AppendValue((byte)1)
                .EndRow();
        }).Should().Throw<DuckDBException>();
    }

    [Fact]
    public void TooManyAppendValueThrowsException()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        using (var duckDbCommand = connection.CreateCommand())
        {
            var table = "CREATE TABLE managedAppenderManyValuesTest(a BOOLEAN, b TINYINT);";
            duckDbCommand.CommandText = table;
            duckDbCommand.ExecuteNonQuery();
        }

        connection.Invoking(dbConnection =>
        {
            using var appender = dbConnection.CreateAppender("managedAppenderManyValuesTest");
            var row = appender.CreateRow();
            row
                .AppendValue(true)
                .AppendValue((byte)1)
                .AppendValue("test")
                .EndRow();

        }).Should().Throw<DuckDBException>();
    }

    [Fact]
    public void WrongTypesThrowException()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        using (var duckDbCommand = connection.CreateCommand())
        {
            var table = "CREATE TABLE managedAppenderWrongTypeTest(a BOOLEAN, c Date, b TINYINT);";
            duckDbCommand.CommandText = table;
            duckDbCommand.ExecuteNonQuery();
        }

        connection.Invoking(dbConnection =>
        {
            using var appender = dbConnection.CreateAppender("managedAppenderWrongTypeTest");
            var row = appender.CreateRow();
            row
                .AppendValue(false)
                .AppendValue((byte)1)
                .AppendValue((short?)1)
                .EndRow();
        }).Should().Throw<DuckDBException>();
    }

    [Fact]
    public void ClosedAdapterThrowException()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        using (var duckDbCommand = connection.CreateCommand())
        {
            var table = "CREATE TABLE managedAppenderClosedAdapterTest(a BOOLEAN, c Date, b TINYINT);";
            duckDbCommand.CommandText = table;
            duckDbCommand.ExecuteNonQuery();
        }

        connection.Invoking(dbConnection =>
        {
            using var appender = dbConnection.CreateAppender("managedAppenderClosedAdapterTest");
            appender.Close();
            var row = appender.CreateRow();
            row
                .AppendValue(false)
                .AppendValue((byte)1)
                .AppendValue((short?)1)
                .EndRow();
        }).Should().Throw<InvalidOperationException>();
    }



    [Fact]
    public void ManagedAppenderTestsWithSchema()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();


        using (var duckDbCommand = connection.CreateCommand())
        {
            var schema = "CREATE SCHEMA managedAppenderTestSchema";
            duckDbCommand.CommandText = schema;
            duckDbCommand.ExecuteNonQuery();
        }

        using (var duckDbCommand = connection.CreateCommand())
        {
            var table = "CREATE TABLE managedAppenderTestSchema.managedAppenderTest(a BOOLEAN, b TINYINT, c SMALLINT, d INTEGER, e BIGINT, f UTINYINT, g USMALLINT, h UINTEGER, i UBIGINT, j REAL, k DOUBLE, l VARCHAR, m Date);";
            duckDbCommand.CommandText = table;
            duckDbCommand.ExecuteNonQuery();
        }

        var rows = 10;
        using (var appender = connection.CreateAppender("managedAppenderTestSchema", "managedAppenderTest"))
        {
            for (var i = 0; i < rows; i++)
            {
                var row = appender.CreateRow();
                row
                    .AppendValue(i % 2 == 0)
                    .AppendValue((sbyte?)i)
                    .AppendValue((short?)i)
                    .AppendValue((int?)i)
                    .AppendValue((long?)i)
                    .AppendValue((byte?)i)
                    .AppendValue((ushort?)i)
                    .AppendValue((uint?)i)
                    .AppendValue((ulong?)i)
                    .AppendValue((float)i)
                    .AppendValue((double)i)
                    .AppendValue($"{i}")
                    .AppendNullValue()
                    .EndRow();
            }
        }

        using (var duckDbCommand = connection.CreateCommand())
        {
            duckDbCommand.CommandText = "SELECT * FROM managedAppenderTestSchema.managedAppenderTest";
            using var reader = duckDbCommand.ExecuteReader();

            var readRowIndex = 0;
            while (reader.Read())
            {
                var booleanCell = (bool)reader[0];

                booleanCell.Should().Be(readRowIndex % 2 == 0);

                for (int columnIndex = 1; columnIndex < 12; columnIndex++)
                {
                    var cell = (IConvertible)reader[columnIndex];
                    cell.ToInt32(CultureInfo.InvariantCulture).Should().Be(readRowIndex);
                }

                readRowIndex++;
            }
            readRowIndex.Should().Be(rows);
        }
    }
}