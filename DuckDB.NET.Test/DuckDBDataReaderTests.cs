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
    public void IndexerValues()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "CREATE TABLE IndexerValuesTests (key INTEGER, value decimal, State Boolean)";
        duckDbCommand.ExecuteNonQuery();

        duckDbCommand.CommandText = "Insert Into IndexerValuesTests values (1, 2.4, true)";
        duckDbCommand.ExecuteNonQuery();

        duckDbCommand.CommandText = "select * from IndexerValuesTests";
        var reader = duckDbCommand.ExecuteReader(CommandBehavior.CloseConnection);

        reader.Read();

        reader[0].Should().Be(reader["key"]);
        reader[1].Should().Be(reader.GetDecimal(1));
        reader.GetValue(2).Should().Be(reader.GetBoolean(2));
    }
}