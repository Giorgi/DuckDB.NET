using Apache.Arrow;
using Apache.Arrow.Types;
using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Test.V2;

// The Arrow appender on the C API V2 inserts each batch with its own INSERT through an internal table function
// (docs/V2-MIGRATION.md, stage 7). These tests pin what that changes from V1, and what the function must keep.
public class V2ArrowAppenderTests
{
    private static readonly Schema IntSchema = new Schema.Builder().Field(f => f.Name("i").DataType(Int32Type.Default)).Build();

    [Fact]
    public void AConstraintFailureDropsOnlyItsBatchAndFaultsTheAppender()
    {
        using var connection = Open();
        connection.ExecuteNonQuery("CREATE TABLE t (i INTEGER NOT NULL)");

        using var appender = connection.CreateArrowAppender("t");
        appender.AppendBatch(Batch(1, 2, 3));

        appender.Invoking(a => a.AppendBatch(Batch(4, null, 6))).Should().Throw<DuckDBException>().WithMessage("*NOT NULL*");
        appender.Invoking(a => a.AppendBatch(Batch(7))).Should().Throw<InvalidOperationException>().WithMessage("*batch failed*");

        Values(connection).Should().Equal(1, 2, 3);
    }

    [Fact]
    public void ClearDoesNothing()
    {
        using var connection = Open();
        connection.ExecuteNonQuery("CREATE TABLE t (i INTEGER)");

        using (var appender = connection.CreateArrowAppender("t"))
        {
            appender.AppendBatch(Batch(1));
            appender.Clear();
            appender.AppendBatch(Batch(2));
        }

        Values(connection).Should().Equal(1, 2);
    }

    [Fact]
    public void TheRowsAreInTheTableWhenAppendBatchReturns()
    {
        using var connection = Open();
        connection.ExecuteNonQuery("CREATE TABLE t (i INTEGER)");

        using var appender = connection.CreateArrowAppender("t");
        using (var batch = Batch(1, 2))
        {
            appender.AppendBatch(batch);
        }

        // The batch is disposed and the appender still open: the rows are in the table.
        Values(connection).Should().Equal(1, 2);
    }

    [Fact]
    public void TheInternalFunctionRefusesACallFromSql()
    {
        using var connection = Open();

        connection.Invoking(c => c.ExecuteNonQuery("SELECT * FROM __duckdbnet_arrow_append(1)"))
                  .Should().Throw<DuckDBException>().WithMessage("*internal to DuckDB.NET's Arrow appender*");
    }

    [Fact]
    public void ARolledBackAppendLeavesTheAppenderWorking()
    {
        // The function is registered when the database is opened, not in the user's transaction, so a rollback does not
        // take it away.
        using var connection = Open();
        connection.ExecuteNonQuery("CREATE TABLE t (i INTEGER)");

        using (var transaction = connection.BeginTransaction())
        {
            using var appender = connection.CreateArrowAppender("t");
            appender.AppendBatch(Batch(1));
            transaction.Rollback();
        }

        using (var appender = connection.CreateArrowAppender("t"))
        {
            appender.AppendBatch(Batch(2));
        }

        Values(connection).Should().Equal(2);
    }

    [Fact]
    public void AppendersOnTwoConnectionsToOneDatabaseInsertTheirOwnBatches()
    {
        var dataSource = $"DataSource=:memory:arrow_{Guid.NewGuid():N}";
        using var first = Open(dataSource);
        using var second = Open(dataSource);
        first.ExecuteNonQuery("CREATE TABLE t (i INTEGER)");

        Parallel.For(0, 2, index =>
        {
            using var appender = (index == 0 ? first : second).CreateArrowAppender("t");
            for (var round = 0; round < 50; round++)
            {
                appender.AppendBatch(Batch(index * 1000 + round));
            }
        });

        Values(first).Should().BeEquivalentTo(Enumerable.Range(0, 50).Concat(Enumerable.Range(1000, 50)));
    }

    [Fact]
    public void AnOpenReaderOnTheConnectionBlocksAnAppend()
    {
        using var connection = Open();
        connection.ExecuteNonQuery("CREATE TABLE t (i INTEGER)");
        using var appender = connection.CreateArrowAppender("t");

        using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT * FROM range(10)";
            using var reader = command.ExecuteReader();
            reader.Read().Should().BeTrue();

            appender.Invoking(a => a.AppendBatch(Batch(1))).Should().Throw<InvalidOperationException>().WithMessage("*open data reader*");
        }

        Values(connection).Should().BeEmpty();
    }

    [Fact]
    public void AnAppenderDisposedAfterItsConnectionClosedDoesNotFail()
    {
        var connection = Open();
        connection.ExecuteNonQuery("CREATE TABLE t (i INTEGER)");
        var appender = connection.CreateArrowAppender("t");
        appender.AppendBatch(Batch(1));

        connection.Dispose();
        appender.Invoking(a => a.Dispose()).Should().NotThrow();
    }

    private static RecordBatch Batch(params int?[] values)
    {
        var builder = new Int32Array.Builder();
        foreach (var value in values)
        {
            if (value is { } v) builder.Append(v); else builder.AppendNull();
        }

        return new RecordBatch(IntSchema, [builder.Build()], values.Length);
    }

    private static List<int> Values(DuckDBConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT i FROM t ORDER BY rowid";
        using var reader = command.ExecuteReader();
        var values = new List<int>();
        while (reader.Read())
        {
            values.Add(reader.GetInt32(0));
        }

        return values;
    }

    private static DuckDBConnection Open(string connectionString = "DataSource=:memory:")
    {
        var connection = new DuckDBConnection(connectionString);
        connection.Open();
        return connection;
    }
}
