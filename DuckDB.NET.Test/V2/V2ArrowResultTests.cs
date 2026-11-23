using Apache.Arrow;
using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Test.V2;

// Arrow results on the C API V2 (statement_execute_arrow), for what the older Arrow tests do not reach.
[Trait("V2", "Ready")]
public class V2ArrowResultTests
{
    [Fact]
    public async Task ParametersAreBound()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT i FROM range($1::BIGINT) t(i) WHERE i % $2 = 0";
        command.Parameters.Add(new DuckDBParameter(100L));
        command.Parameters.Add(new DuckDBParameter(10L));

        (await Values(command)).Should().Equal(0, 10, 20, 30, 40, 50, 60, 70, 80, 90);
    }

    [Fact]
    public async Task APreparedCommandIsExecutedForArrow()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT i FROM range($1::BIGINT) t(i)";
        command.Parameters.Add(new DuckDBParameter(3L));
        command.Prepare();

        var entry = connection.GetOrPrepareStatement(command.CommandText);
        var executions = entry.ExecutionCount;

        (await Values(command)).Should().Equal(0, 1, 2);
        command.Parameters[0].Value = 5L;
        (await Values(command)).Should().Equal(0, 1, 2, 3, 4);

        entry.ExecutionCount.Should().Be(executions + 2, "both executions use the prepared statement");
    }

    [Fact]
    public async Task TheStatementsBeforeTheFirstResultSetRun()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE t AS SELECT 7::BIGINT AS v; INSERT INTO t VALUES (8); SELECT v FROM t ORDER BY v";

        (await Values(command)).Should().Equal(7, 8);
    }

    [Fact]
    public async Task TheExceptionOfACSharpFunctionIsTheInnerException()
    {
        using var connection = Open();
        var thrown = new InvalidOperationException("from the function");
        connection.RegisterScalarFunction<long, long>("fails_late", (long x) => x < 400_000 ? x : throw thrown);

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT fails_late(i) FROM range(500000) t(i)";

        var act = () => Values(command);
        (await act.Should().ThrowAsync<DuckDBException>()).Which.InnerException.Should().BeSameAs(thrown);
    }

    [Fact]
    public async Task TheConnectionIsBusyUntilTheStreamIsDisposed()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM range(10)";

        using (var stream = command.ExecuteArrowStream())
        {
            (await stream.ReadNextRecordBatchAsync()).Should().NotBeNull();
            connection.Invoking(c => c.ExecuteNonQuery("SELECT 1")).Should().Throw<InvalidOperationException>().WithMessage("*open data reader*");
        }

        connection.ExecuteNonQuery("SELECT 1");
    }

    private static DuckDBConnection Open()
    {
        var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        return connection;
    }

    private static async Task<List<long>> Values(DuckDBCommand command)
    {
        var values = new List<long>();
        await foreach (var batch in command.ExecuteArrowBatchesAsync())
        {
            using (batch)
            {
                var column = (Int64Array)batch.Column(0);
                for (var row = 0; row < column.Length; row++)
                {
                    values.Add(column.GetValue(row)!.Value);
                }
            }
        }

        return values;
    }
}
