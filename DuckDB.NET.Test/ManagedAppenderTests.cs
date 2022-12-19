using System;
using System.Globalization;
using DuckDB.NET.Data;
using Xunit;
using FluentAssertions;

namespace DuckDB.NET.Test
{
    public class DuckDBManagedAppenderTests
    {
        [Fact]
        public void ManagedAppenderTests()
        {
            using var connection = new DuckDBConnection("DataSource=:memory:");
            connection.Open();

            using (var duckDbCommand = connection.CreateCommand())
            {
                var table = "CREATE TABLE managedAppenderTest(a BOOLEAN, b TINYINT, c SMALLINT, d INTEGER, e BIGINT, f UTINYINT, g USMALLINT, h UINTEGER, i UBIGINT, j REAL, k DOUBLE, l VARCHAR);";
                duckDbCommand.CommandText = table;
                duckDbCommand.ExecuteNonQuery();
            }

            var rows = 10;
            using (var appender = connection.CreateAppender("managedAppenderTest"))
            {
                for (var i = 0; i < rows; i++)
                {
                    using var row = appender.CreateRow();
                    row
                        .AppendValue(i % 2 == 0)
                        .AppendValue((byte)i)
                        .AppendValue((short)i)
                        .AppendValue((int)i)
                        .AppendValue((long)i)
                        .AppendValue((byte)i)
                        .AppendValue((ushort)i)
                        .AppendValue((uint)i)
                        .AppendValue((ulong)i)
                        .AppendValue((float)i)
                        .AppendValue((double)i)
                        .AppendValue($"{i}");
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
                using var row = appender.CreateRow();
                row
                    .AppendValue(true)
                    .AppendValue((byte)1);
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
                using var row = appender.CreateRow();
                row
                    .AppendValue(true)
                    .AppendValue((byte)1);
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
                using var row = appender.CreateRow();
                row
                    .AppendValue(true)
                    .AppendValue((byte)1)
                    .AppendValue("test");

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
                using var row = appender.CreateRow();
                row
                    .AppendValue(false)
                    .AppendValue((byte)1)
                    .AppendValue((short)1);
            }).Should().Throw<DuckDBException>();
        }
    }
}

