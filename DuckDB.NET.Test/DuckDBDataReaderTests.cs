using System;
using System.Data;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBDataReaderTests
{
    [Fact]
    public void GetOrdinalReturnsColumnIndex()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "CREATE TABLE GetOrdinalTests (key INTEGER, value TEXT, State Boolean)";
        duckDbCommand.ExecuteNonQuery();

        duckDbCommand.CommandText = "select * from GetOrdinalTests";
        var reader = duckDbCommand.ExecuteReader();

        reader.GetOrdinal("key").Should().Be(0);
        reader.GetOrdinal("value").Should().Be(1);

        reader.Invoking(dataReader => dataReader.GetOrdinal("Random")).Should().Throw<DuckDBException>();
    }

    [Fact]
    public void CloseConnectionClosesConnection()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "CREATE TABLE CloseConnectionTests (key INTEGER, value TEXT, State Boolean)";
        duckDbCommand.ExecuteNonQuery();

        duckDbCommand.CommandText = "select * from CloseConnectionTests";
        var reader = duckDbCommand.ExecuteReader(CommandBehavior.CloseConnection);
        reader.Close();

        reader.IsClosed.Should().BeTrue();
        connection.State.Should().Be(ConnectionState.Closed);
    }

    [Fact]
    public void ReaderValues()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "CREATE TABLE IndexerValuesTests (key INTEGER, value decimal, State Boolean, ErrorCode Integer)";
        duckDbCommand.ExecuteNonQuery();

        duckDbCommand.CommandText = "Insert Into IndexerValuesTests values (1, 2.4, true, null)";
        duckDbCommand.ExecuteNonQuery();

        duckDbCommand.CommandText = "select * from IndexerValuesTests";
        var reader = duckDbCommand.ExecuteReader(CommandBehavior.CloseConnection);

        reader.Read();

        reader.HasRows.Should().BeTrue();
        reader[0].Should().Be(reader["key"]);
        reader[1].Should().Be(reader.GetDecimal(1));
        reader.GetValue(2).Should().Be(reader.GetBoolean(2));
        reader[3].Should().Be(DBNull.Value);
        
        var values = new object[4];
        reader.GetValues(values);
        values.Should().BeEquivalentTo(new object[] { 1, 2.4, true, DBNull.Value });

        reader.GetFieldType(1).Should().Be(typeof(decimal));
        reader.GetFieldType(2).Should().Be(typeof(bool));
    }

    [Fact]
    public void ReaderEnumerator()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var duckDbCommand = connection.CreateCommand();

        duckDbCommand.CommandText = "select 2 union select 4";
        var reader = duckDbCommand.ExecuteReader(CommandBehavior.CloseConnection);

        var enumerator = reader.GetEnumerator();

        enumerator.MoveNext().Should().Be(true);

        (enumerator.Current as IDataRecord).GetInt32(0).Should().Be(2);
        enumerator.MoveNext().Should().Be(true);

        (enumerator.Current as IDataRecord).GetInt32(0).Should().Be(4);
        enumerator.MoveNext().Should().Be(false);
    }

    [Fact]
    public void ReadIntervalValues()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "SELECT INTERVAL 1 YEAR;";

        var reader = duckDbCommand.ExecuteReader();
        reader.Read();
        reader.GetFieldType(0).Should().Be(typeof(DuckDBInterval));
        reader.GetDataTypeName(0).Should().Be("DuckdbTypeInterval");

        var interval = reader.GetFieldValue<DuckDBInterval>(0);

        interval.Months.Should().Be(12);

        duckDbCommand.CommandText = "SELECT INTERVAL '28' DAYS;";
        reader = duckDbCommand.ExecuteReader();
        reader.Read();

        interval = reader.GetFieldValue<DuckDBInterval>(0);

        interval.Days.Should().Be(28);

        duckDbCommand.CommandText = "SELECT INTERVAL 30 SECONDS;";
        reader = duckDbCommand.ExecuteReader();
        reader.Read();

        interval = reader.GetFieldValue<DuckDBInterval>(0);

        interval.Micros.Should().Be(30_000_000);
    }
    
    [Fact]
    public void LoadDataTable()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "select 1 as num, 'text' as str, TIMESTAMP '1992-09-20 20:38:40' as tme";
        var reader = duckDbCommand.ExecuteReader();
        var dt = new DataTable();
        dt.Load(reader);
        dt.Rows.Count.Should().Be(1);
    }
}
