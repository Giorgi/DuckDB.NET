using System.Threading;
using Apache.Arrow;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Test.Helpers;

namespace DuckDB.NET.Test;

// DuckDBCommand.Prepare() keeps the statement on the connection, and later executions only bind and execute.
public class PreparedCommandTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void PreparedCommandExecutesRepeatedlyWithNewValues()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT $1::INTEGER + 1";
        command.Parameters.Add(new DuckDBParameter(0));
        command.Prepare();

        for (var i = 0; i < 5; i++)
        {
            command.Parameters[0].Value = i;
            command.ExecuteScalar().Should().Be(i + 1);
        }
    }

    [Fact]
    public void PreparedInsertAndUpdateReportAffectedRows()
    {
        Connection.ExecuteNonQuery("CREATE TABLE prepared_rows (id INTEGER, name VARCHAR)");

        using var insert = Connection.CreateCommand();
        insert.CommandText = "INSERT INTO prepared_rows VALUES ($1, $2)";
        insert.Parameters.Add(new DuckDBParameter(0));
        insert.Parameters.Add(new DuckDBParameter("a"));
        insert.Prepare();

        for (var i = 0; i < 3; i++)
        {
            insert.Parameters[0].Value = i;
            insert.Parameters[1].Value = "name" + i;
            insert.ExecuteNonQuery().Should().Be(1);
        }

        using var update = Connection.CreateCommand();
        update.CommandText = "UPDATE prepared_rows SET name = $1 WHERE id >= $2";
        update.Parameters.Add(new DuckDBParameter("changed"));
        update.Parameters.Add(new DuckDBParameter(1));
        update.Prepare();

        update.ExecuteNonQuery().Should().Be(2);
        update.Parameters[1].Value = 0;
        update.ExecuteNonQuery().Should().Be(3);

        Connection.Query<string>("SELECT name FROM prepared_rows ORDER BY rowid").Should().Equal("changed", "changed", "changed");
    }

    [Fact]
    public void PrepareReportsAnErrorInTheText()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT ? FROM prepared_nowhere";

        command.Invoking(c => c.Prepare()).Should().Throw<DuckDBException>().WithMessage("*prepared_nowhere*");
    }

    // While a value is bound, DuckDB reports an untyped parameter as the type of that value. Reading the types after
    // an execution without clearing the bindings would convert 1.5 to the BIGINT of the previous value and return 2.
    [Fact]
    public void UntypedParameterTakesEachValueAsItIs()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT $1";
        command.Parameters.Add(new DuckDBParameter(42L));
        command.Prepare();

        command.ExecuteScalar().Should().Be(42L);

        command.Parameters[0].Value = 1.5;
        command.ExecuteScalar().Should().Be(1.5);

        command.Parameters[0].Value = "text";
        command.ExecuteScalar().Should().Be("text");
    }

    [Fact]
    public void TypedParameterConvertsEachValue()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT $1::INTEGER * 2";
        command.Parameters.Add(new DuckDBParameter(3));
        command.Prepare();

        command.ExecuteScalar().Should().Be(6);

        command.Parameters[0].Value = 4L;
        command.ExecuteScalar().Should().Be(8);

        command.Parameters[0].Value = (short)5;
        command.ExecuteScalar().Should().Be(10);
    }

    [Fact]
    public void NamedParametersAreBoundOnEachExecution()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT $a::INTEGER - $b::INTEGER";
        command.Parameters.Add(new DuckDBParameter("a", 10));
        command.Parameters.Add(new DuckDBParameter("b", 3));
        command.Prepare();

        command.ExecuteScalar().Should().Be(7);

        command.Parameters["a"].Value = 20;
        command.Parameters["b"].Value = 5;
        command.ExecuteScalar().Should().Be(15);
    }

    // The bindings of the previous execution are cleared, so a parameter that no longer gets a value is reported,
    // instead of keeping the value it had.
    [Fact]
    public void NamedParameterWithoutAValueIsReportedMissing()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT $a::INTEGER + $b::INTEGER";
        command.Parameters.Add(new DuckDBParameter("a", 1));
        command.Parameters.Add(new DuckDBParameter("b", 2));
        command.Prepare();

        command.ExecuteScalar().Should().Be(3);

        command.Parameters[1].ParameterName = "c";
        command.Invoking(c => c.ExecuteScalar()).Should().Throw<DuckDBException>().WithMessage("*b*");
    }

    [Fact]
    public void ChangingCommandTextUndoesThePreparation()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT 1";
        command.Prepare();
        command.ExecuteScalar().Should().Be(1);

        command.CommandText = "SELECT 2";
        command.ExecuteScalar().Should().Be(2);

        command.Prepare();
        command.ExecuteScalar().Should().Be(2);
    }

    [Fact]
    public void CommandsWithTheSameTextShareTheStatement()
    {
        using var first = Connection.CreateCommand();
        using var second = Connection.CreateCommand();
        first.CommandText = second.CommandText = "SELECT $1::INTEGER";
        first.Parameters.Add(new DuckDBParameter(1));
        second.Parameters.Add(new DuckDBParameter(2));
        first.Prepare();
        second.Prepare();

        for (var i = 0; i < 3; i++)
        {
            first.ExecuteScalar().Should().Be(1);
            second.ExecuteScalar().Should().Be(2);
        }
    }

    [Fact]
    public void ExecutingAgainWhileAReaderIsOpen()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT * FROM range($1::BIGINT) r(i)";
        command.Parameters.Add(new DuckDBParameter(3L));
        command.Prepare();

        using var firstReader = command.ExecuteReader();
        command.Parameters[0].Value = 5L;
        using var secondReader = command.ExecuteReader();

        Count(firstReader).Should().Be(3);
        Count(secondReader).Should().Be(5);
    }

    [Fact]
    public void PreparedCommandInStreamingMode()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT * FROM range($1::BIGINT) r(i)";
        command.Parameters.Add(new DuckDBParameter(5000L));
        command.UseStreamingMode = true;
        command.Prepare();

        for (var i = 0; i < 3; i++)
        {
            using var reader = command.ExecuteReader();
            Count(reader).Should().Be(5000);
        }
    }

    [Fact]
    public void CommandOfSeveralStatementsIsPreparedOnEachExecution()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "CREATE TABLE IF NOT EXISTS prepared_multi (i INTEGER); INSERT INTO prepared_multi VALUES (1); SELECT count(*) FROM prepared_multi";
        command.Prepare();

        command.ExecuteScalar().Should().Be(1L);
        command.ExecuteScalar().Should().Be(2L);
    }

    // DuckDB expands a dynamic PIVOT when it runs, so a prepared one still sees new values as new columns.
    [Fact]
    public void PreparedDynamicPivotSeesNewColumns()
    {
        Connection.ExecuteNonQuery("CREATE TABLE prepared_sales (region VARCHAR, yr INTEGER, amount INTEGER); INSERT INTO prepared_sales VALUES ('a', 2020, 1), ('b', 2021, 2)");

        using var command = Connection.CreateCommand();
        command.CommandText = "PIVOT prepared_sales ON yr USING sum(amount)";
        command.Prepare();

        using (var reader = command.ExecuteReader())
        {
            reader.FieldCount.Should().Be(3);
        }

        Connection.ExecuteNonQuery("INSERT INTO prepared_sales VALUES ('c', 2022, 3)");

        using (var reader = command.ExecuteReader())
        {
            reader.FieldCount.Should().Be(4);
        }
    }

    [Fact]
    public void PreparedCommandIsPreparedAgainAfterTheConnectionReopens()
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT $1::INTEGER + 1";
        command.Parameters.Add(new DuckDBParameter(1));
        command.Prepare();
        command.ExecuteScalar().Should().Be(2);

        connection.Close();
        command.Invoking(c => c.ExecuteScalar()).Should().Throw<InvalidOperationException>();

        connection.Open();
        command.ExecuteScalar().Should().Be(2);
    }

    // CloseConnection closes the connection from the reader, which destroys the statement; the next open prepares it again.
    [Fact]
    public void PreparedCommandWithCloseConnectionBehavior()
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT $1::INTEGER";
        command.Parameters.Add(new DuckDBParameter(7));
        command.Prepare();

        using (var reader = command.ExecuteReader(CommandBehavior.CloseConnection))
        {
            reader.Read().Should().BeTrue();
            reader.GetInt32(0).Should().Be(7);
        }

        connection.State.Should().Be(ConnectionState.Closed);

        connection.Open();
        command.ExecuteScalar().Should().Be(7);
    }

    // The statement must be gone before the connection returns the native database: a second instance on the same
    // file would lose data on Linux and macOS and fail with "file in use" on Windows. On DuckDB 2.0 a prepared
    // statement no longer keeps the database alive, so this passes there even without the release in Close(); it
    // stays to catch a DuckDB version where it does again.
    [Fact]
    public void ClosingTheConnectionReleasesTheDatabaseOfAPreparedStatement()
    {
        using var file = DisposableFile.GenerateInTemp("db");

        var connection = new DuckDBConnection(file.ConnectionString);
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE t (i INTEGER)");

        var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO t VALUES ($1)";
        command.Parameters.Add(new DuckDBParameter(1));
        command.Prepare();
        command.ExecuteNonQuery().Should().Be(1);

        // The command is neither disposed nor executed again: closing the connection alone releases its statement.
        connection.Close();

        using (var second = new DuckDBConnection(file.ConnectionString))
        {
            second.Open();
            second.ExecuteNonQuery("INSERT INTO t VALUES (2)");
        }

        using var third = new DuckDBConnection(file.ConnectionString);
        third.Open();
        third.Query<int>("SELECT i FROM t ORDER BY rowid").Should().Equal(1, 2);

        GC.KeepAlive(command);
    }

    [Fact]
    public void PreparedCommandMovedToAnotherConnection()
    {
        using var firstFile = DisposableFile.GenerateInTemp("db");
        using var secondFile = DisposableFile.GenerateInTemp("db");

        using var first = new DuckDBConnection(firstFile.ConnectionString);
        using var second = new DuckDBConnection(secondFile.ConnectionString);
        first.Open();
        second.Open();
        first.ExecuteNonQuery("CREATE TABLE t AS SELECT 'first' AS source");
        second.ExecuteNonQuery("CREATE TABLE t AS SELECT 'second' AS source");

        using var command = first.CreateCommand();
        command.CommandText = "SELECT source FROM t";
        command.Prepare();
        command.ExecuteScalar().Should().Be("first");

        command.Connection = second;
        command.ExecuteScalar().Should().Be("second");

        command.Prepare();
        command.ExecuteScalar().Should().Be("second");
    }

    // A failed preparation after a reopen is raised inside the reader's constructor, which closes the connection.
    [Fact]
    public void FailedPrepareAfterReopenStillClosesTheConnection()
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE gone (i INTEGER)");

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM gone";
        command.Prepare();

        // A private in-memory database is gone when its connection closes, and the table with it.
        connection.Close();
        connection.Open();

        command.Invoking(c => c.ExecuteReader(CommandBehavior.CloseConnection)).Should().Throw<DuckDBException>();
        connection.State.Should().Be(ConnectionState.Closed);
    }

    // While the shared statement is taken, a command with the same text prepares a statement of its own.
    [Fact]
    public void TakenStatementFallsBackToPreparingAgain()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT $1::INTEGER + 100";
        command.Parameters.Add(new DuckDBParameter(1));
        command.Prepare();

        var entry = Connection.GetOrPrepareStatement(command.CommandText);
        entry.TryRent().Should().BeTrue();
        try
        {
            command.ExecuteScalar().Should().Be(101);
        }
        finally
        {
            entry.Return();
        }

        command.Parameters[0].Value = 2;
        command.ExecuteScalar().Should().Be(102);
    }

    [Fact]
    public void PreparedCommandAsArrowStream()
    {
        using var command = Connection.CreateCommand();
        command.CommandText = "SELECT * FROM range($1::BIGINT) r(i)";
        command.Parameters.Add(new DuckDBParameter(10L));
        command.Prepare();

        for (var count = 10L; count <= 20; count += 10)
        {
            command.Parameters[0].Value = count;
            using var stream = command.ExecuteArrowStream();

            var rows = 0L;
            while (stream.ReadNextRecordBatchAsync().AsTask().Result is { } batch)
            {
                using (batch)
                {
                    rows += ((Int64Array)batch.Column(0)).Length;
                }
            }

            rows.Should().Be(count);
        }
    }

    // Two commands with the same text on two threads of one connection: one of them binds the shared statement, the
    // other prepares a statement of its own while it is taken.
    [Fact]
    public void CommandsWithTheSameTextOnTwoThreads()
    {
        using var connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();

        var failures = 0;
        var threads = Enumerable.Range(0, 2).Select(thread => new Thread(() =>
        {
            try
            {
                using var command = connection.CreateCommand();
                command.CommandText = "SELECT $1::INTEGER * 10 + $2::INTEGER";
                command.Parameters.Add(new DuckDBParameter(0));
                command.Parameters.Add(new DuckDBParameter(thread));
                command.Prepare();

                for (var i = 0; i < 200; i++)
                {
                    command.Parameters[0].Value = i;
                    if ((int)command.ExecuteScalar() != i * 10 + thread)
                    {
                        Interlocked.Increment(ref failures);
                    }
                }
            }
            catch
            {
                Interlocked.Increment(ref failures);
            }
        })).ToList();

        threads.ForEach(t => t.Start());
        threads.ForEach(t => t.Join());

        failures.Should().Be(0);
    }

    private static int Count(DuckDBDataReader reader)
    {
        var count = 0;
        while (reader.Read())
        {
            count++;
        }

        return count;
    }
}
