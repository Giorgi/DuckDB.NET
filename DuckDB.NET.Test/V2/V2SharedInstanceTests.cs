using System.IO;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Test.Helpers;

namespace DuckDB.NET.Test.V2;

// DuckDB shares the instances on the C API V2 (instance_open_shared, docs/V2-MIGRATION.md stage 1): one per database
// file, as DuckDB resolves its path, and one per named in-memory database.
public class V2SharedInstanceTests
{
    [Fact]
    public void TwoSpellingsOfOneFileShareTheDatabase()
    {
        using var file = DisposableFile.GenerateInTemp("db");
        var directory = Path.GetDirectoryName(file.FileName)!;
        var name = Path.GetFileName(file.FileName);

        using var first = Open(file.ConnectionString);
        first.ExecuteNonQuery("CREATE TABLE t AS SELECT 7 AS v");

        using var second = Open($"DataSource={Path.Combine(directory, ".", name)}");
        Scalar(second, "SELECT v FROM t").Should().Be(7);
    }

    [Fact]
    public void AFileOpenWithOtherOptionsIsRefused()
    {
        using var file = DisposableFile.GenerateInTemp("db");
        using var first = Open(file.ConnectionString);

        var refused = new DuckDBConnection($"{file.ConnectionString};ACCESS_MODE=READ_ONLY");
        var exception = refused.Invoking(c => c.Open()).Should().Throw<DuckDBException>().Which;
        exception.NativeErrorCode.Should().Be(DuckDBErrorCode.ResourceInUse);
        refused.State.Should().Be(System.Data.ConnectionState.Closed);
    }

    [Fact]
    public void TheSameOptionsSpelledDifferentlyShareTheDatabase()
    {
        using var file = DisposableFile.GenerateInTemp("db");

        using var first = Open($"{file.ConnectionString};threads=3");
        first.ExecuteNonQuery("CREATE TABLE t AS SELECT 7 AS v");

        using var second = Open($"{file.ConnectionString};threads=03");
        Scalar(second, "SELECT v FROM t").Should().Be(7);
    }

    [Fact]
    public void AClosedFileCanBeOpenedAgainAndDeleted()
    {
        using var file = DisposableFile.GenerateInTemp("db");

        using (var connection = Open(file.ConnectionString))
        {
            connection.ExecuteNonQuery("CREATE TABLE t AS SELECT 7 AS v");
        }

        using (var connection = Open(file.ConnectionString))
        {
            Scalar(connection, "SELECT v FROM t").Should().Be(7);
        }

        // The last connection closed the instance, which released the file.
        File.Delete(file.FileName);
        File.Exists(file.FileName).Should().BeFalse();
    }

    [Fact]
    public void ANamedInMemoryDatabaseIsSharedByName()
    {
        var name = $":memory:shared_{Guid.NewGuid():N}";

        using var first = Open($"DataSource={name}");
        first.ExecuteNonQuery("CREATE TABLE t AS SELECT 7 AS v");

        using var second = Open($"DataSource={name}");
        Scalar(second, "SELECT v FROM t").Should().Be(7);

        using var other = Open($"DataSource=:memory:other_{Guid.NewGuid():N}");
        other.Invoking(c => Scalar(c, "SELECT v FROM t")).Should().Throw<DuckDBException>().WithMessage("*t does not exist*");
    }

    [Fact]
    public void ANamedInMemoryDatabaseIsGoneAfterItsLastConnection()
    {
        var name = $"DataSource=:memory:gone_{Guid.NewGuid():N}";

        using (var connection = Open(name))
        {
            connection.ExecuteNonQuery("CREATE TABLE t AS SELECT 7 AS v");
        }

        using var again = Open(name);
        again.Invoking(c => Scalar(c, "SELECT v FROM t")).Should().Throw<DuckDBException>().WithMessage("*t does not exist*");
    }

    [Fact]
    public void APlainInMemoryDatabaseIsPrivate()
    {
        using var first = Open("DataSource=:memory:");
        first.ExecuteNonQuery("CREATE TABLE private_table AS SELECT 7 AS v");

        using var second = Open("DataSource=:memory:");
        second.Invoking(c => Scalar(c, "SELECT v FROM private_table")).Should().Throw<DuckDBException>().WithMessage("*does not exist*");
    }

    [Fact]
    public void TheSharedInMemoryFormIsMatchedInAnyLetterCase()
    {
        // The shared in-memory database is the whole test run's, so the table gets a name of its own.
        var table = $"t_{Guid.NewGuid():N}";

        using var lower = Open("DataSource=:memory:?cache=shared");
        lower.ExecuteNonQuery($"CREATE TABLE {table} AS SELECT 7 AS v");

        using var upper = Open("DataSource=:MEMORY:?CACHE=SHARED");
        Scalar(upper, $"SELECT v FROM {table}").Should().Be(7);

        lower.ExecuteNonQuery($"DROP TABLE {table}");
    }

    [Theory]
    [InlineData(":memory:")]
    [InlineData(":memory:duplicated")]
    public void ADuplicateReachesTheSameDatabase(string dataSource)
    {
        var source = dataSource == ":memory:" ? dataSource : $"{dataSource}_{Guid.NewGuid():N}";

        using var connection = Open($"DataSource={source}");
        connection.ExecuteNonQuery("CREATE TABLE t AS SELECT 7 AS v");

        using var duplicate = connection.Duplicate();
        duplicate.Open();
        Scalar(duplicate, "SELECT v FROM t").Should().Be(7);

        // The database outlives the connection it was duplicated from.
        connection.Close();
        Scalar(duplicate, "SELECT v FROM t").Should().Be(7);
    }

    private static DuckDBConnection Open(string connectionString)
    {
        var connection = new DuckDBConnection(connectionString);
        connection.Open();
        return connection;
    }

    private static object Scalar(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar();
    }
}
