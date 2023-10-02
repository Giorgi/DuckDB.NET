using System;
using System.Collections.Generic;
using System.Data;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBDataReaderTests : DuckDBTestBase
{
    public DuckDBDataReaderTests(DuckDBDatabaseFixture db) : base(db)
    {
    }

    [Fact]
    public void GetOrdinalReturnsColumnIndex()
    { 
        Command.CommandText = "CREATE TABLE GetOrdinalTests (key INTEGER, value TEXT, State Boolean)";
        Command.ExecuteNonQuery();

        Command.CommandText = "select * from GetOrdinalTests";
        var reader = Command.ExecuteReader();

        reader.GetOrdinal("key").Should().Be(0);
        reader.GetOrdinal("value").Should().Be(1);

        reader.Invoking(dataReader => dataReader.GetOrdinal("Random")).Should().Throw<DuckDBException>();
    }

    [Fact]
    public void CloseConnectionClosesConnection()
    {
        Command.CommandText = "CREATE TABLE CloseConnectionTests (key INTEGER, value TEXT, State Boolean)";
        Command.ExecuteNonQuery();

        Command.CommandText = "select * from CloseConnectionTests";
        var reader = Command.ExecuteReader(CommandBehavior.CloseConnection);
        reader.Close();

        reader.IsClosed.Should().BeTrue();
        Connection.State.Should().Be(ConnectionState.Closed);
    }

    [Fact]
    public void ReaderValues()
    {
        Command.CommandText = "CREATE TABLE IndexerValuesTests (key INTEGER, value decimal, State Boolean, ErrorCode Integer, mean Float, stdev double)";
        Command.ExecuteNonQuery();

        Command.CommandText = "Insert Into IndexerValuesTests values (1, 2.4, true, null, null, null)";
        Command.ExecuteNonQuery();

        Command.CommandText = "select * from IndexerValuesTests";
        var reader = Command.ExecuteReader(CommandBehavior.CloseConnection);

        reader.Read();

        reader.HasRows.Should().BeTrue();
        reader[0].Should().Be(reader["key"]);
        reader[1].Should().Be(reader.GetDecimal(1));
        reader.GetValue(2).Should().Be(reader.GetBoolean(2));
        reader[3].Should().Be(DBNull.Value);

        var values = new object[6];
        reader.GetValues(values);
        values.Should().BeEquivalentTo(new object[] { 1, 2.4, true, DBNull.Value, DBNull.Value, DBNull.Value });

        reader.GetFieldType(1).Should().Be(typeof(decimal));
        reader.GetFieldType(2).Should().Be(typeof(bool));
        reader.GetFieldType(4).Should().Be(typeof(float));
        reader.GetFieldType(5).Should().Be(typeof(double));
    }

    [Fact]
    public void ReaderEnumerator()
    {
        Command.CommandText = "select 7 union select 11 order by 1";
        using var reader = Command.ExecuteReader(CommandBehavior.CloseConnection);
        var enumerator = reader.GetEnumerator();

        enumerator.MoveNext().Should().Be(true);
        (enumerator.Current as IDataRecord).GetInt32(0).Should().Be(7);

        enumerator.MoveNext().Should().Be(true);
        (enumerator.Current as IDataRecord).GetInt32(0).Should().Be(11);

        enumerator.MoveNext().Should().Be(false);
    }

    [Fact]
    public void ReadIntervalValues()
    {
        Command.CommandText = "SELECT INTERVAL 1 YEAR;";

        var reader = Command.ExecuteReader();
        reader.Read();
        reader.GetFieldType(0).Should().Be(typeof(DuckDBInterval));
        reader.GetDataTypeName(0).Should().Be(DuckDBType.Interval.ToString());

        var interval = reader.GetFieldValue<DuckDBInterval>(0);

        interval.Months.Should().Be(12);

        Command.CommandText = "SELECT INTERVAL '28' DAYS;";
        reader = Command.ExecuteReader();
        reader.Read();

        interval = reader.GetFieldValue<DuckDBInterval>(0);

        interval.Days.Should().Be(28);

        Command.CommandText = "SELECT INTERVAL 30 SECONDS;";
        reader = Command.ExecuteReader();
        reader.Read();

        interval = reader.GetFieldValue<DuckDBInterval>(0);

        interval.Micros.Should().Be(30_000_000);
    }

    [Fact]
    public void LoadDataTable()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        Command.CommandText = "select 1 as num, 'text' as str, TIMESTAMP '1992-09-20 20:38:40' as tme";
        var reader = Command.ExecuteReader();
        var dt = new DataTable();
        dt.Load(reader);
        dt.Rows.Count.Should().Be(1);
    }

    [Fact]
    public void MultipleStatementsQueryData()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        Command.CommandText = "Select 1; Select 2";

        using var reader = Command.ExecuteReader();

        reader.Read();
        reader.GetInt32(0).Should().Be(1);

        reader.NextResult().Should().BeTrue();

        reader.Read().Should().BeTrue();

        reader.GetInt32(0).Should().Be(2);

        reader.NextResult().Should().BeFalse();
    }

    [Fact]
    public void ReadManyRows()
    {
        var table = "CREATE TABLE TableForManyRows(foo INTEGER, bar VARCHAR);";
        Command.CommandText = table;
        Command.ExecuteNonQuery();

        var rows = 10_000;

        var values = new List<KeyValuePair<int?, string>>();

        using (var appender = Connection.CreateAppender("TableForManyRows"))
        {
            for (var i = 0; i < rows; i++)
            {
                var value = new string((char)('A' + i % 26), Random.Shared.Next(2, 20));
                values.Add(new KeyValuePair<int?, string>(i, value));

                var row = appender.CreateRow();

                row
                    .AppendValue(i)
                    .AppendValue(value)
                    .EndRow();
            }
            values.Add(new KeyValuePair<int?, string>(null, null));

            appender.CreateRow().AppendNullValue().AppendNullValue().EndRow();
        }

        Command.CommandText = "SELECT * FROM TableForManyRows";
        using var reader = Command.ExecuteReader();

        var readRowIndex = 0;
        while (reader.Read())
        {
            var item = values[readRowIndex];

            (reader.IsDBNull(0) ? (int?)null : reader.GetInt32(0)).Should().Be(item.Key);
            (reader.IsDBNull(1) ? null : reader.GetString(1)).Should().Be(item.Value);

            readRowIndex++;
        }

        readRowIndex.Should().Be(rows + 1);
    }
}
