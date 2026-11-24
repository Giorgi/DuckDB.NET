using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Test.V2;

// The appender on the C API V2 buffers rows in a column data collection and inserts them when it flushes
// (docs/V2-MIGRATION.md, "The appender"). These tests pin what that design has to keep.
public class V2AppenderTests
{
    [Fact]
    public void RowsThatLeaveDifferentColumnsToTheirDefaultsKeepTheirOrder()
    {
        using var connection = Open();
        connection.ExecuteNonQuery("CREATE SEQUENCE ids; CREATE TABLE t (id INTEGER DEFAULT nextval('ids'), name VARCHAR DEFAULT 'none', n INTEGER)");

        // Three patterns that change every few rows, over more rows than a chunk holds.
        const int rows = 5000;
        using (var appender = connection.CreateAppender("t"))
        {
            for (var i = 0; i < rows; i++)
            {
                appender.AppendRow((i % 7) switch
                {
                    < 3 => row => row.AppendValue((int?)-i).AppendValue($"row {i}").AppendValue((int?)i),
                    < 5 => row => row.AppendDefault().AppendValue($"row {i}").AppendValue((int?)i),
                    _ => row => row.AppendDefault().AppendDefault().AppendValue((int?)i),
                });
            }
        }

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT id, name, n FROM t ORDER BY rowid";
        using var reader = command.ExecuteReader();

        var sequence = 0;
        for (var i = 0; i < rows; i++)
        {
            reader.Read().Should().BeTrue();
            reader.GetInt32(2).Should().Be(i);

            switch (i % 7)
            {
                case < 3:
                    reader.GetInt32(0).Should().Be(-i);
                    reader.GetString(1).Should().Be($"row {i}");
                    break;
                case < 5:
                    reader.GetInt32(0).Should().Be(++sequence);
                    reader.GetString(1).Should().Be($"row {i}");
                    break;
                default:
                    reader.GetInt32(0).Should().Be(++sequence);
                    reader.GetString(1).Should().Be("none");
                    break;
            }
        }

        reader.Read().Should().BeFalse();
    }

    [Fact]
    public void ARowCanLeaveEveryColumnToItsDefault()
    {
        using var connection = Open();
        connection.ExecuteNonQuery("CREATE TABLE t (a INTEGER DEFAULT 7, b VARCHAR DEFAULT 'x')");

        using (var appender = connection.CreateAppender("t"))
        {
            appender.AppendRow(row => row.AppendValue((int?)1).AppendValue("one"));
            appender.AppendRow(row => row.AppendDefault().AppendDefault());
            appender.AppendRow(row => row.AppendDefault().AppendDefault());
            appender.AppendRow(row => row.AppendValue((int?)4).AppendValue("four"));
        }

        Rows(connection, "SELECT a, b FROM t ORDER BY rowid").Should().Equal("1 one", "7 x", "7 x", "4 four");
    }

    [Fact]
    public void TheBufferIsFlushedEvery204800Rows()
    {
        using var connection = Open();
        connection.ExecuteNonQuery("CREATE TABLE t (n INTEGER)");

        using (var appender = connection.CreateAppender("t"))
        {
            for (var i = 0; i < 204_800 + 3000; i++)
            {
                appender.AppendRow(i, static (row, value) => row.AppendValue((int?)value));
            }

            // The first 204,800 rows are in the table; the rest wait in the buffer for the next flush or the close.
            Scalar(connection, "SELECT count(*) FROM t").Should().Be(204_800L);
        }

        Scalar(connection, "SELECT count(*) FROM t").Should().Be(207_800L);
        Scalar(connection, "SELECT sum(n)::BIGINT FROM t").Should().Be(207_800L * 207_799 / 2);
    }

    [Fact]
    public void AFailedFlushDropsTheRowsSinceTheLastFlush()
    {
        using var connection = Open();
        connection.ExecuteNonQuery("CREATE TABLE t (id INTEGER PRIMARY KEY)");

        var appender = connection.CreateAppender("t");
        appender.AppendRow(row => row.AppendValue((int?)1));
        appender.AppendRow(row => row.AppendValue((int?)1));

        appender.Invoking(a => a.Close()).Should().Throw<DuckDBException>().WithMessage("*Constraint*");
        Scalar(connection, "SELECT count(*) FROM t").Should().Be(0L);
    }

    [Fact]
    public void AFlushWhileAReaderIsOpenOnTheConnectionIsRefused()
    {
        using var connection = Open();
        connection.ExecuteNonQuery("CREATE TABLE t (n INTEGER)");

        var appender = connection.CreateAppender("t");
        appender.AppendRow(row => row.AppendValue((int?)1));

        using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT * FROM range(10)";
            using var reader = command.ExecuteReader();
            reader.Read().Should().BeTrue();

            appender.Invoking(a => a.Close()).Should().Throw<InvalidOperationException>().WithMessage("*open data reader*");
        }

        Scalar(connection, "SELECT count(*) FROM t").Should().Be(0L);
    }

    [Fact]
    public void TableAndColumnNamesAreQuoted()
    {
        using var connection = Open();
        connection.ExecuteNonQuery(""""CREATE SCHEMA "my schema"; CREATE TABLE "my schema"."a ""quoted"" table" ("first col" INTEGER, "say ""hi""" VARCHAR DEFAULT 'hi')"""");

        using (var appender = connection.CreateAppender("my schema", "a \"quoted\" table"))
        {
            appender.AppendRow(row => row.AppendValue((int?)1).AppendValue("one"));
            appender.AppendRow(row => row.AppendValue((int?)2).AppendDefault());
        }

        Rows(connection, """"SELECT "first col", "say ""hi""" FROM "my schema"."a ""quoted"" table" ORDER BY rowid"""").Should().Equal("1 one", "2 hi");
    }

    [Fact]
    public void AnAppenderForATableThatDoesNotExistIsRefused()
    {
        using var connection = Open();
        connection.Invoking(c => c.CreateAppender("missing")).Should().Throw<DuckDBException>().WithMessage("*missing*");
    }

    private static DuckDBConnection Open()
    {
        var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        return connection;
    }

    private static object Scalar(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar();
    }

    private static List<string> Rows(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();
        var rows = new List<string>();
        while (reader.Read())
        {
            rows.Add($"{reader.GetValue(0)} {reader.GetValue(1)}");
        }

        return rows;
    }
}
