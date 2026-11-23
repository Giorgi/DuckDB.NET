using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Test.V2;

// Stage 2 of docs/V2-MIGRATION.md: queries through the C API V2, where every result streams, a connection has one
// open result, and a statement runs while its result is read.
[Trait("V2", "Ready")]
public class V2QueryTests
{
    private static DuckDBConnection Open()
    {
        var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        return connection;
    }

    [Fact]
    public void StreamingIsTheDefault()
    {
        new DuckDBCommand().UseStreamingMode.Should().BeTrue();
    }

    [Fact]
    public void MaterializedResultsAreRefused()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1";
        command.UseStreamingMode = false;

        command.Invoking(c => c.ExecuteReader()).Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void AnotherCommandWhileAReaderIsOpenIsRefused()
    {
        using var connection = Open();

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT * FROM range(10000)";

        using (var reader = command.ExecuteReader())
        {
            reader.Read().Should().BeTrue();

            using var other = connection.CreateCommand();
            other.CommandText = "SELECT 42";

            var exception = other.Invoking(c => c.ExecuteScalar()).Should().Throw<InvalidOperationException>().Which;
            exception.InnerException.Should().BeOfType<DuckDBException>()
                     .Which.NativeErrorCode.Should().Be(DuckDBErrorCode.ResourceInUse);
        }

        // Closing the reader frees the connection.
        using var next = connection.CreateCommand();
        next.CommandText = "SELECT 42";
        next.ExecuteScalar().Should().Be(42);
    }

    [Fact]
    public void ReadsEveryRowOfSeveralChunks()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT i::INTEGER AS i, 'row ' || i AS s FROM range(5000) t(i) ORDER BY i";

        using var reader = command.ExecuteReader();

        reader.GetName(0).Should().Be("i");
        reader.GetName(1).Should().Be("s");
        reader.GetFieldType(0).Should().Be(typeof(int));

        var rows = 0;
        while (reader.Read())
        {
            reader.GetInt32(0).Should().Be(rows);
            reader.GetString(1).Should().Be($"row {rows}");
            rows++;
        }

        rows.Should().Be(5000);
    }

    [Fact]
    public void AnEmptyResultStillHasItsColumns()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 AS a, 'x' AS b WHERE false";

        using var reader = command.ExecuteReader();

        reader.HasRows.Should().BeFalse();
        reader.FieldCount.Should().Be(2);
        reader.GetName(1).Should().Be("b");
        reader.GetFieldType(1).Should().Be(typeof(string));
        reader.Read().Should().BeFalse();
    }

    [Fact]
    public void DdlRunThroughExecuteReaderIsApplied()
    {
        using var connection = Open();

        using (var command = connection.CreateCommand())
        {
            command.CommandText = "CREATE TABLE created_by_reader(i INTEGER)";
            using var reader = command.ExecuteReader();
        }

        connection.ExecuteScalar<long>("SELECT count(*) FROM information_schema.tables WHERE table_name = 'created_by_reader'").Should().Be(1);
    }

    [Fact]
    public void AnInsertReturningLeftEarlyIsAppliedAndCounted()
    {
        using var connection = Open();
        connection.ExecuteNonQuery("CREATE TABLE returning_rows(i INTEGER)");

        using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO returning_rows SELECT * FROM range(5000) RETURNING i";

        using (var reader = command.ExecuteReader())
        {
            reader.Read().Should().BeTrue();

            // Leaving the result early runs the INSERT to its end, as when results were materialized with V1.
            reader.NextResult().Should().BeFalse();
            reader.RecordsAffected.Should().Be(5000);
        }

        connection.ExecuteScalar<long>("SELECT count(*) FROM returning_rows").Should().Be(5000);
    }

    [Fact]
    public void ASelectLeftEarlyDoesNotRunToItsEnd()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();

        // Running this to its end would take minutes; closing the reader aborts it instead.
        command.CommandText = "SELECT i FROM range(10000000000) t(i)";

        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        using (var reader = command.ExecuteReader())
        {
            reader.Read().Should().BeTrue();
        }

        stopwatch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(10));
        connection.ExecuteScalar<int>("SELECT 1").Should().Be(1);
    }

    [Fact]
    public void AnErrorInALateRowIsReportedByRead()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:;threads=1");
        connection.Open();
        connection.ExecuteNonQuery("SET streaming_buffer_size = '100KB'");

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT CASE WHEN i = 100000 THEN error('late row') ELSE i END FROM range(200000) t(i)";

        using var reader = command.ExecuteReader();

        var act = () =>
        {
            while (reader.Read())
            {
            }
        };

        act.Should().Throw<DuckDBException>().WithMessage("*late row*");
    }

    [Fact]
    public void ExecuteNonQueryCountsEveryStatement()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE counted(i INTEGER); INSERT INTO counted VALUES (1), (2), (3); UPDATE counted SET i = i + 1 WHERE i > 1; SELECT 1";

        command.ExecuteNonQuery().Should().Be(5);
    }

    [Fact]
    public void ASyntaxErrorIsReportedWithItsCode()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELEC 1";

        var exception = command.Invoking(c => c.ExecuteNonQuery()).Should().Throw<DuckDBException>().Which;

        exception.NativeErrorCode.Should().Be(DuckDBErrorCode.QueryParser);
        exception.ErrorType.Should().Be(DuckDBErrorType.Parser);
    }

    [Fact]
    public void AStatementExpandedIntoSeveralIsRead()
    {
        using var connection = Open();
        connection.ExecuteNonQuery("CREATE TABLE sales(city VARCHAR, year INTEGER, amount INTEGER); " +
                                   "INSERT INTO sales VALUES ('A', 2020, 1), ('A', 2021, 2), ('B', 2020, 3)");

        using var command = connection.CreateCommand();
        command.CommandText = "PIVOT sales ON year USING sum(amount)::INTEGER GROUP BY city ORDER BY city";

        using var reader = command.ExecuteReader();

        reader.FieldCount.Should().Be(3);
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("A");
    }
}
