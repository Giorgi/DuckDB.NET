using DuckDB.NET.Data;
using DuckDB.NET.Native;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBDataReaderTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void GetOrdinalReturnsColumnIndex()
    {
        Command.CommandText = "CREATE TABLE GetOrdinalTests (key INTEGER, value TEXT, State Boolean)";
        Command.ExecuteNonQuery();

        Command.CommandText = "select * from GetOrdinalTests";
        Command.UseStreamingMode = true;
        var reader = Command.ExecuteReader();

        reader.GetOrdinal("key").Should().Be(0);
        reader.GetOrdinal("value").Should().Be(1);

        reader.Invoking(dataReader => dataReader.GetOrdinal("Random")).Should().Throw<DuckDBException>();
    }

    [Fact]
    public void GetOrdinalRepeatedColumnReturnsFirstIndex()
    {
        Command.CommandText = "CREATE TABLE GetOrdinalTests (key INTEGER, value TEXT, State Boolean)";
        Command.ExecuteNonQuery();

        Command.CommandText = "select value, key, value from GetOrdinalTests";
        Command.UseStreamingMode = true;
        var reader = Command.ExecuteReader();

        reader.GetOrdinal("key").Should().Be(1);
        reader.GetOrdinal("value").Should().Be(0);

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
    public void ReadValueBeforeReadThrowsException()
    {
        Command.CommandText = "select 24";
        var reader = Command.ExecuteReader();

        reader.Invoking(r => r.IsDBNull(0)).Should().Throw<InvalidOperationException>();
        reader.Invoking(r => r.GetValue(0)).Should().Throw<InvalidOperationException>();
        reader.Invoking(r => r.GetFieldValue<int>(0)).Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ReaderValues()
    {
        Command.CommandText = "CREATE TABLE IndexerValuesTests (key INTEGER, value decimal, State Boolean, ErrorCode Integer, mean Float, stdev double)";
        Command.ExecuteNonQuery();

        Command.CommandText = "Insert Into IndexerValuesTests values (1, 2.4, true, null, null, null)";
        Command.ExecuteNonQuery();

        Command.CommandText = "Insert Into IndexerValuesTests values (2, 4.8, null, null, null, null)";
        Command.ExecuteNonQuery();

        Command.CommandText = "Insert Into IndexerValuesTests values (3, null, null, null, null, null)";
        Command.ExecuteNonQuery();

        Command.CommandText = "select * from IndexerValuesTests";
        var reader = Command.ExecuteReader(CommandBehavior.CloseConnection);

        reader.Read();

        reader.HasRows.Should().BeTrue();
        reader[0].Should().Be(reader["key"]);
        reader[1].Should().Be(reader.GetDecimal(1));
        reader.GetValue(2).Should().Be(reader.GetBoolean(2));
        reader.GetFieldValue<bool?>(2).Should().Be(reader.GetBoolean(2));
        reader[3].Should().Be(DBNull.Value);

        var values = new object[6];
        reader.GetValues(values);
        values.Should().BeEquivalentTo(new object[] { 1, 2.4, true, DBNull.Value, DBNull.Value, DBNull.Value });

        reader.GetFieldType(1).Should().Be(typeof(decimal));
        reader.GetFieldType(2).Should().Be(typeof(bool));
        reader.GetFieldType(4).Should().Be(typeof(float));
        reader.GetFieldType(5).Should().Be(typeof(double));

        reader.Read();
        reader.GetDecimal(1).Should().Be(4.8m);
        reader.GetFieldValue<bool?>(2).Should().BeNull();

        reader.Invoking(dataReader => dataReader.GetFieldValue<bool>(2)).Should().Throw<InvalidCastException>();
        reader.Invoking(dataReader => dataReader.GetFieldValue<int>(3)).Should().Throw<InvalidCastException>();

        reader.Read();
        reader.GetFieldValue<decimal?>(1).Should().BeNull();
        reader.Invoking(dataReader => dataReader.GetFieldValue<decimal>(1)).Should().Throw<InvalidCastException>();
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
        reader.GetFieldType(0).Should().Be(typeof(TimeSpan));
        reader.GetDataTypeName(0).Should().Be(DuckDBType.Interval.ToString());

        var interval = reader.GetFieldValue<DuckDBInterval>(0);
        reader.Invoking(r => r.GetValue(0)).Should().Throw<ArgumentOutOfRangeException>();

        interval.Months.Should().Be(12);

        Command.CommandText = "SELECT INTERVAL '28' DAYS;";
        reader = Command.ExecuteReader();
        reader.Read();

        interval = reader.GetFieldValue<DuckDBInterval>(0);
        var value = (TimeSpan)reader.GetValue(0);

        var timeSpan = reader.GetFieldValue<TimeSpan>(0);
        timeSpan.Days.Should().Be(28);

        interval.Days.Should().Be(28);
        value.Days.Should().Be(28);

        Command.CommandText = "SELECT INTERVAL 30 SECONDS;";
        reader = Command.ExecuteReader();
        reader.Read();

        interval = reader.GetFieldValue<DuckDBInterval>(0);
        timeSpan = (TimeSpan)reader.GetValue(0);

        interval.Micros.Should().Be(30_000_000);
        timeSpan.Should().Be(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void LoadDataTable()
    {
        Command.CommandText = "select 1 as num, 'text' as str, TIMESTAMP '1992-09-20 20:38:40' as tme";
        var reader = Command.ExecuteReader();
        var dt = new DataTable();
        dt.Load(reader);
        dt.Rows.Count.Should().Be(1);
    }

    [Fact]
    public void MultipleStatementsQueryData()
    {
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
    public void MultipleStatementsQueryDataFromAll()
    {
        Command.CommandText = "Select 1; Select 2 where 1=0; Select 3";

        using var reader = Command.ExecuteReader();

        //Select 1
        reader.Read();
        reader.GetInt32(0).Should().Be(1);

        //Select 2 where 1=0
        reader.NextResult().Should().BeTrue();
        reader.HasRows.Should().BeFalse();
        reader.Read().Should().BeFalse();

        //Select 3
        reader.NextResult().Should().BeTrue();
        reader.HasRows.Should().BeTrue();
        reader.Read().Should().BeTrue();

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

    [Fact]
    public void ReadDateAsDateTime()
    {
        Command.CommandText = "CREATE TABLE intdate(foo INTEGER, bar DATE);";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO intdate VALUES (3, date '2001-02-03'), (5, date '2004-05-06'), (7, date '2007-08-09');";
        Command.ExecuteNonQuery();

        Command.CommandText = "SELECT bar FROM intdate";
        using var reader = Command.ExecuteReader();

        var dates = new List<DateTime>();

        while (reader.Read())
        {
            for (int c = 0; c < reader.FieldCount; c++)
            {
                dates.Add(reader.GetDateTime(c));
            }
        }

        dates.Should().BeEquivalentTo(new List<DateTime> { new(2001, 2, 3), new(2004, 5, 6), new(2007, 8, 9) });
    }

    [Fact]
    public void ReadPivotStatementResult()
    {
        Command.CommandText = "CREATE TABLE Cities(Country VARCHAR, Name VARCHAR, Year INT, Population INT);";
        Command.ExecuteNonQuery();

        Command.CommandText = "Insert into Cities Values ('Georgia', 'საქართველო', 2022, 3688647)";
        Command.ExecuteNonQuery();

        Command.CommandText = "PIVOT Cities ON Year USING SUM(Population);";
        var reader = Command.ExecuteReader();

        reader.Read();
        reader.HasRows.Should().BeTrue();

        reader.NextResult().Should().BeFalse();
    }

    [Fact]
    public void ReadInsertReturningClause()
    {
        Command.CommandText = "CREATE TABLE t2 (i INT, j INT);";
        Command.ExecuteNonQuery();

        Command.UseStreamingMode = true;
        Command.CommandText = @"INSERT INTO t2 
                                    SELECT 2 AS i, 3 AS j 
                                    RETURNING *, i * j AS i_times_j;";

        var reader = Command.ExecuteReader();

        reader.Read();
        reader.HasRows.Should().BeTrue();

        reader.NextResult().Should().BeFalse();
    }

    [Fact]
    public void ReadNonQueryAsResult()
    {
        Command.CommandText = "CREATE TABLE IndexerValuesTests (key INTEGER, value decimal, State Boolean, ErrorCode Integer, mean Float, stdev double)";
        var reader = Command.ExecuteReader();
        reader.HasRows.Should().BeFalse();

        reader.Invoking(r => r.Close()).Should().NotThrow();
    }

    [Fact]
    public void ReadDecimalSchema()
    {
        Command.CommandText = "CREATE TABLE decimaltbl(foo decimal(10,2));";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO decimaltbl VALUES (3.45), (9.35), (7.24);";
        Command.ExecuteNonQuery();

        Command.CommandText = "SELECT foo FROM decimaltbl";
        using var reader = Command.ExecuteReader();

        var schemaTable = reader.GetSchemaTable();
        schemaTable.Rows[0]["NumericScale"].Should().Be(2);
        schemaTable.Rows[0]["NumericPrecision"].Should().Be(10);
    }

    [Fact]
    public void ReadDecimalSchemaWithoutTableRow()
    {
        Command.CommandText = "CREATE TABLE decimaltbl(foo decimal(10,2));";
        Command.ExecuteNonQuery();

        Command.CommandText = "SELECT foo FROM decimaltbl";
        using var reader = Command.ExecuteReader();

        var schemaTable = reader.GetSchemaTable();
        schemaTable.Rows[0]["NumericScale"].Should().Be(0);
        schemaTable.Rows[0]["NumericPrecision"].Should().Be(0);
    }

    [Fact]
    public async Task CancellingLongRunningQueryThrowsOperationCancelledException()
    {
        Command.CommandText = @"create table cnt as WITH RECURSIVE
                       cnt(x) AS (
                          SELECT 1
                          UNION ALL
                          SELECT x+1 FROM cnt
                           where x < 300000
                    ) select * from cnt;";

        var source = new CancellationTokenSource(1000);

        await Command.Invoking(async c => await c.ExecuteReaderAsync(source.Token)).Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public void ReadVarint()
    {
        Command.CommandText = "SELECT (-1234)::VARINT";

        var reader = Command.ExecuteReader();
        reader.Read();
        var value = (BigInteger)reader.GetValue(0);
    }
}
