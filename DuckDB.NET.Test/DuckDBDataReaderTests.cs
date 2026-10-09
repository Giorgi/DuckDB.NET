using System.Threading;

namespace DuckDB.NET.Test;

public class DuckDBDataReaderTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Theory]
    [InlineData("SELECT 1")]
    [InlineData("SELECT 1::HUGEINT")]
    [InlineData("SELECT 'a'")]
    [InlineData("SELECT 1.5::DECIMAL(4, 2)")]
    [InlineData("SELECT TIMESTAMP '2024-05-05 12:00:30'")]
    [InlineData("SELECT DATE '2024-05-05'")]
    [InlineData("SELECT true")]
    [InlineData("SELECT uuid()")]
    [InlineData("SELECT 'a'::ENUM('a', 'b')")]
    [InlineData("SELECT {'x': 1}")]
    [InlineData("SELECT [1, 2]")]
    [InlineData("SELECT MAP {'k': 1}")]
    public void GetFieldValueOfObjectMatchesGetValue(string query)
    {
        Command.CommandText = query;
        using var reader = Command.ExecuteReader();
        reader.Read();

        var expected = reader.GetValue(0);
        var value = reader.GetFieldValue<object>(0);

        value.Should().BeOfType(expected.GetType());
        value.Should().BeEquivalentTo(expected);
    }

    [Fact]
    public void GetFieldValueOfObjectReturnsDBNullForNull()
    {
        Command.CommandText = "SELECT NULL::INTEGER";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<object>(0).Should().Be(DBNull.Value);
    }

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
    public void CloseConnectionReleasesTheDatabaseBeforeTheConnectionReportsClosed()
    {
        // With CommandBehavior.CloseConnection the reader must release its statements before closing the
        // connection. When that connection is the file's last one, closing it closes the database; if the
        // reader's statements are still alive they keep the native instance alive, and a connection opened
        // meanwhile gets a second instance on the same file. On Linux both instances then write the file
        // and committed rows are lost; on Windows the second open is refused.
        // Opening a connection from the reader connection's Closed event lands in exactly that window.
        var file = Path.Combine(Path.GetTempPath(), $"closeconnection-{Guid.NewGuid():N}.db");
        var connectionString = $"Data Source={file}";

        void Execute(DuckDBConnection connection, string sql)
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }

        try
        {
            using (var setup = new DuckDBConnection(connectionString))
            {
                setup.Open();
                Execute(setup, "CREATE TABLE CloseConnectionRelease (id INTEGER)");
            }

            using var connection = new DuckDBConnection(connectionString);
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT id FROM CloseConnectionRelease";
            var reader = command.ExecuteReader(CommandBehavior.CloseConnection);

            // A committed write through the same database, so there is something to lose.
            using (var writer = new DuckDBConnection(connectionString))
            {
                writer.Open();
                Execute(writer, "INSERT INTO CloseConnectionRelease VALUES (1)");
            }

            while (reader.Read())
            {
            }

            connection.StateChange += (_, e) =>
            {
                if (e.CurrentState != ConnectionState.Closed)
                {
                    return;
                }

                using var other = new DuckDBConnection(connectionString);
                other.Open();
                Execute(other, "INSERT INTO CloseConnectionRelease VALUES (2)");
            };

            reader.Dispose();

            using var verify = new DuckDBConnection(connectionString);
            verify.Open();
            using var count = verify.CreateCommand();
            count.CommandText = "SELECT count(*) FROM CloseConnectionRelease";
            Convert.ToInt32(count.ExecuteScalar()).Should().Be(2);
        }
        finally
        {
            File.Delete(file);
            File.Delete(file + ".wal");
        }
    }

    [Theory]
    [InlineData("SELEC 1", CommandBehavior.CloseConnection, ConnectionState.Closed, false)]
    [InlineData("SELEC 1", CommandBehavior.Default, ConnectionState.Open, false)]
    [InlineData("SELECT CAST('not a number' AS INTEGER)", CommandBehavior.CloseConnection, ConnectionState.Closed, false)]
    [InlineData("SELECT CAST('not a number' AS INTEGER)", CommandBehavior.Default, ConnectionState.Open, false)]
    [InlineData("SELECT CAST('not a number' AS INTEGER)", CommandBehavior.CloseConnection, ConnectionState.Closed, true)]
    [InlineData("SELECT CAST('not a number' AS INTEGER)", CommandBehavior.Default, ConnectionState.Open, true)]
    public void FailedExecuteReaderClosesConnectionOnlyWithCloseConnection(string sql, CommandBehavior behavior, ConnectionState expectedState, bool useStreamingMode)
    {
        // Matches Npgsql: the caller handed the connection to a reader it never received.
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.UseStreamingMode = useStreamingMode;

        command.Invoking(c => c.ExecuteReader(behavior)).Should().Throw<DuckDBException>();

        connection.State.Should().Be(expectedState);
    }

    [Fact]
    public void StreamingReadFailureClosesConnectionOnlyWhenReaderIsDisposed()
    {
        // Matches Npgsql: a failed Read() leaves the connection open, and disposing the reader closes it.
        using var connection = OpenConnectionForLateStreamingErrors();
        using var command = connection.CreateCommand();
        command.UseStreamingMode = true;
        command.CommandText = "SELECT CAST(CASE WHEN i < 500000 THEN CAST(i AS VARCHAR) ELSE 'not a number' END AS INTEGER) FROM range(1000000) t(i)";

        var reader = command.ExecuteReader(CommandBehavior.CloseConnection);

        reader.Invoking(r => { while (r.Read()) { } }).Should().Throw<DuckDBException>();
        connection.State.Should().Be(ConnectionState.Open);

        reader.Dispose();
        connection.State.Should().Be(ConnectionState.Closed);
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

    // Regression tests for https://github.com/Giorgi/DuckDB.NET/issues/358
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ReadAfterEndOfResultReturnsFalse(bool useStreamingMode)
    {
        Command.UseStreamingMode = useStreamingMode;
        Command.CommandText = "SELECT i FROM range(5000) t(i)";

        using var reader = Command.ExecuteReader();

        var rows = 0;
        while (reader.Read())
        {
            rows++;
        }

        rows.Should().Be(5000);
        reader.Read().Should().BeFalse();
        reader.Read().Should().BeFalse();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ReadOnEmptyResultReturnsFalse(bool useStreamingMode)
    {
        Command.UseStreamingMode = useStreamingMode;
        Command.CommandText = "SELECT i FROM range(0) t(i)";

        using var reader = Command.ExecuteReader();

        reader.HasRows.Should().BeFalse();
        reader.Read().Should().BeFalse();
        reader.Read().Should().BeFalse();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void NextResultAfterReadingPastEndReturnsNextResultSet(bool useStreamingMode)
    {
        Command.UseStreamingMode = useStreamingMode;
        Command.CommandText = "SELECT i FROM range(0) t(i); SELECT 42";

        using var reader = Command.ExecuteReader();

        reader.Read().Should().BeFalse();
        reader.Read().Should().BeFalse();

        reader.NextResult().Should().BeTrue();
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(42);
        reader.Read().Should().BeFalse();
        reader.Read().Should().BeFalse();
    }

    [Fact]
    public void StreamingReadThrowsWhenLaterChunkFails()
    {
        // The first chunks convert fine; the error is raised only while producing a later chunk.
        using var connection = OpenConnectionForLateStreamingErrors();
        using var command = connection.CreateCommand();
        command.UseStreamingMode = true;
        command.CommandText = "SELECT CAST(CASE WHEN i < 500000 THEN CAST(i AS VARCHAR) ELSE 'not a number' END AS INTEGER) FROM range(1000000) t(i)";

        using var reader = command.ExecuteReader();

        var rows = 0;
        var act = () =>
        {
            while (reader.Read())
            {
                rows++;
            }
        };

        act.Should().Throw<DuckDBException>().Where(e => e.ErrorType == DuckDBErrorType.Conversion);
        rows.Should().BeGreaterThan(0);

        // The failed fetch freed the previous chunk, so the reader must not serve values from it.
        reader.Invoking(r => r.GetInt32(0)).Should().Throw<InvalidOperationException>();
        reader.Invoking(r => r.Read()).Should().Throw<DuckDBException>();
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

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Repro332_SchemaTableAfterNextResult(bool streaming)
    {
        Command.CommandText = "SELECT 1 AS a, 2 AS b";
        Command.UseStreamingMode = streaming;
        using var reader = Command.ExecuteReader();

        while (reader.Read()) { }
        reader.NextResult().Should().BeFalse(); // exhausts result set, like DataTable.Load does

        var schemaTable = reader.GetSchemaTable();
        schemaTable.Rows.Count.Should().Be(2);
    }

    [Fact]
    public void Repro332_DataTableLoad()
    {
        Command.CommandText = "SELECT 1 AS a, 2 AS b";
        using var reader = Command.ExecuteReader();

        var table = new DataTable();
        table.Load(reader); // calls NextResult() internally after reading rows

        var schemaTable = reader.GetSchemaTable();
        schemaTable.Rows.Count.Should().Be(2);
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CancellingLongRunningQueryThrowsOperationCancelledException(bool useStreamingMode)
    {
        Command.CommandText = @"create table cnt as WITH RECURSIVE
                       cnt(x) AS (
                          SELECT 1
                          UNION ALL
                          SELECT x+1 FROM cnt
                           where x < 300000
                    ) select * from cnt;";

        var source = new CancellationTokenSource(1000);

        Command.UseStreamingMode = useStreamingMode;
        await Command.Invoking(async c => await c.ExecuteReaderAsync(source.Token)).Should().ThrowAsync<OperationCanceledException>();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void CancellingLongRunningSyncQueryByTokenRegistrationThrowsOperationCanceledException(bool useStreamingMode)
    {
        Command.CommandText = @"create table cnt as WITH RECURSIVE
                   cnt(x) AS (
                      SELECT 1
                      UNION ALL
                      SELECT x+1 FROM cnt
                       where x < 300000
                ) select * from cnt;";

        using var source = new CancellationTokenSource(TimeSpan.FromMilliseconds(1000));

        Command.UseStreamingMode = useStreamingMode;
        Command.Invoking(c =>
        {
            source.Token.Register(() => c.Cancel());

            using var reader = c.ExecuteReader();
            while (reader.Read())
            {
                _ = reader.GetInt32(0);
            }

        }).Should().Throw<OperationCanceledException>();
    }

    [Fact]
    public void CancelDuringStreamingReadThrowsOperationCanceledException()
    {
        const long totalRows = 10_000_000;

        Command.UseStreamingMode = true;
        Command.CommandText = $"SELECT i FROM range({totalRows}) t(i)";

        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();

        // Cancel after ExecuteReader has returned, so the interrupt surfaces from a later fetch rather than from execute.
        Command.Cancel();

        var rows = 1L;
        reader.Invoking(r =>
        {
            while (r.Read())
            {
                rows++;
            }
        }).Should().Throw<OperationCanceledException>();

        rows.Should().BeLessThan(totalRows);
    }

    [Fact]
    public void ReadVarint()
    {
        Command.CommandText = "SELECT (-1234)::VARINT";

        var reader = Command.ExecuteReader();
        reader.Read();
        var value = (BigInteger)reader.GetValue(0);
    }

    [Fact]
    public void ReadHugeIntAsDuckDBHugeInt()
    {
        Command.CommandText = "SELECT 170141183460469231731687303715884105727::HUGEINT, (-170141183460469231731687303715884105728)::HUGEINT, (-5)::HUGEINT, " +
                              "340282366920938463463374607431768211455::UHUGEINT, 42::UHUGEINT, NULL::HUGEINT";

        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DuckDBHugeInt>(0).ToBigInteger().Should().Be(DuckDBHugeInt.HugeIntMaxValue);
        reader.GetFieldValue<DuckDBHugeInt>(1).ToBigInteger().Should().Be(DuckDBHugeInt.HugeIntMinValue);
        reader.GetFieldValue<DuckDBHugeInt>(2).ToBigInteger().Should().Be(new BigInteger(-5));
        reader.GetFieldValue<DuckDBHugeInt?>(2).Value.ToBigInteger().Should().Be(new BigInteger(-5));

        reader.GetFieldValue<DuckDBUHugeInt>(3).ToBigInteger().Should().Be(DuckDBUHugeInt.HugeIntMaxValue);
        reader.GetFieldValue<DuckDBUHugeInt>(4).ToBigInteger().Should().Be(new BigInteger(42));

        reader.GetFieldValue<DuckDBHugeInt?>(5).Should().BeNull();
        reader.Invoking(r => r.GetFieldValue<DuckDBHugeInt>(5)).Should().Throw<InvalidCastException>();

        // The two structs are not interchangeable: the signed one cannot hold every unsigned value.
        reader.Invoking(r => r.GetFieldValue<DuckDBUHugeInt>(2)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFieldValue<DuckDBHugeInt>(4)).Should().Throw<InvalidCastException>();

        // The reader names the structs as the provider-specific types, so it has to return them.
        reader.GetProviderSpecificFieldType(0).Should().Be(typeof(DuckDBHugeInt));
        reader.GetProviderSpecificValue(0).Should().BeOfType<DuckDBHugeInt>().Which.ToBigInteger().Should().Be(DuckDBHugeInt.HugeIntMaxValue);
        reader.GetProviderSpecificValue(1).Should().BeOfType<DuckDBHugeInt>().Which.ToBigInteger().Should().Be(DuckDBHugeInt.HugeIntMinValue);
        reader.GetProviderSpecificValue(2).Should().BeOfType<DuckDBHugeInt>().Which.ToBigInteger().Should().Be(new BigInteger(-5));
        reader.GetProviderSpecificFieldType(3).Should().Be(typeof(DuckDBUHugeInt));
        reader.GetProviderSpecificValue(3).Should().BeOfType<DuckDBUHugeInt>().Which.ToBigInteger().Should().Be(DuckDBUHugeInt.HugeIntMaxValue);
        reader.GetProviderSpecificValue(4).Should().BeOfType<DuckDBUHugeInt>().Which.ToBigInteger().Should().Be(new BigInteger(42));
        reader.GetProviderSpecificValue(5).Should().Be(DBNull.Value);

        // Reading as BigInteger is unchanged.
        reader.GetValue(0).Should().Be(DuckDBHugeInt.HugeIntMaxValue);
        reader.GetValue(3).Should().Be(DuckDBUHugeInt.HugeIntMaxValue);
        reader.GetFieldValue<BigInteger>(0).Should().Be(DuckDBHugeInt.HugeIntMaxValue);
        reader.GetFieldValue<BigInteger>(3).Should().Be(DuckDBUHugeInt.HugeIntMaxValue);
    }
}
