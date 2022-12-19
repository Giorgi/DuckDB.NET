using System;
using System.Globalization;
using DuckDB.NET;
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
		         
		         var table = "CREATE TABLE appenderTest(a BOOLEAN, b TINYINT, c SMALLINT, d INTEGER, e BIGINT, f UTINYINT, g USMALLINT, h UINTEGER, i UBIGINT, j REAL, k DOUBLE, l VARCHAR);";
		         duckDbCommand.CommandText = table;
		         duckDbCommand.ExecuteNonQuery();
	         }

		     var rows = 10;
	         using (var appender = connection.CreateAppender("appenderTest"))
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
		         duckDbCommand.CommandText = "SELECT * FROM appenderTest";
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
		         
		         var table = "CREATE TABLE appenderTest(a BOOLEAN, b TINYINT, c SMALLINT, d INTEGER, e BIGINT, f UTINYINT, g USMALLINT, h UINTEGER, i UBIGINT, j REAL, k DOUBLE, l VARCHAR);";
		         duckDbCommand.CommandText = table;
		         duckDbCommand.ExecuteNonQuery();
	         }

	         Assert.Throws<DuckDBException>(() =>
	         {
		         var rows = 10;
		         using var appender = connection.CreateAppender("appenderTest");
		         for (var i = 0; i < rows; i++)
		         {
			         using var row = appender.CreateRow();
			         row
				         .AppendValue(i % 2 == 0)
				         .AppendValue((byte) i);
		         }
	         });
         }
         
         [Fact]
         public void TableDoesNotExistsThrowsException()
         {
	         using var connection = new DuckDBConnection("DataSource=:memory:");
	         connection.Open();

	         Assert.Throws<DuckDBException>(() =>
	         {
		         var rows = 10;
		         using var appender = connection.CreateAppender("appenderTest");
		         for (var i = 0; i < rows; i++)
		         {
			         using var row = appender.CreateRow();
			         row
				         .AppendValue(i % 2 == 0)
				         .AppendValue((byte) i);
		         }
	         });
         }
         
         [Fact]
         public void TooManyAppendValueThrowsException()
         {
	         using var connection = new DuckDBConnection("DataSource=:memory:");
	         connection.Open();

	         using (var duckDbCommand = connection.CreateCommand())
	         {
		         
		         var table = "CREATE TABLE appenderTest(a BOOLEAN, b TINYINT);";
		         duckDbCommand.CommandText = table;
		         duckDbCommand.ExecuteNonQuery();
	         }

	         Assert.Throws<DuckDBException>(() =>
	         {
		         var rows = 10;
		         using var appender = connection.CreateAppender("appenderTest");
		         for (var i = 0; i < rows; i++)
		         {
			         using var row = appender.CreateRow();
			         row
				         .AppendValue(i % 2 == 0)
				         .AppendValue((byte) i)
				         .AppendValue((short) i);
		         }
	         });
         }
         
         [Fact]
         public void WrongTypesThrowException()
         {
	         using var connection = new DuckDBConnection("DataSource=:memory:");
	         connection.Open();

	         using (var duckDbCommand = connection.CreateCommand())
	         {
		         
		         var table = "CREATE TABLE appenderTest(a BOOLEAN, c Date, b TINYINT);";
		         duckDbCommand.CommandText = table;
		         duckDbCommand.ExecuteNonQuery();
	         }

	         Assert.Throws<DuckDBException>(() =>
	         {
		         var rows = 10;
		         using var appender = connection.CreateAppender("appenderTest");
		         for (var i = 0; i < rows; i++)
		         {
			         using var row = appender.CreateRow();
			         row
				         .AppendValue(i % 2 == 0)
				         .AppendValue((byte) i)
				         .AppendValue((short) i);
		         }
	         });
         }
    }
}

