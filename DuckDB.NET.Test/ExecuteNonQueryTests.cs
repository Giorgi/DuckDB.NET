using DuckDB.NET.Test.Helpers;

namespace DuckDB.NET.Test;

public class ExecuteNonQueryTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void TableQueryTest()
    {
        Command.CommandText = "CREATE TABLE users (id INTEGER, name TEXT);";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO users VALUES (1, 'user1'), (2, 'user2'), (3, 'user3'), (4, 'user4');";
        var affectedRows = Command.ExecuteNonQuery();
        affectedRows.Should().Be(4);

        Command.CommandText = "UPDATE users SET name = 'unnamed' WHERE id % 2 = 0;";
        affectedRows = Command.ExecuteNonQuery();
        affectedRows.Should().Be(2);
    }

    [Fact]
    public void ExecuteQueryWithUnicodeText()
    {
        var words = new List<string> { "张三", "李四", "王五", "გიორგი" };

        Command.CommandText = "CREATE TABLE test(id BIGINT, name STRING); ";
        Command.ExecuteNonQuery();

        Command.CommandText = $"INSERT INTO test VALUES (1, '{words[0]}'), (3, '{words[1]}'), (5, '{words[2]}'), (4, '{words[3]}')";
        Command.ExecuteNonQuery().Should().Be(4);

        Command.CommandText = "Select * from test";
        using (var reader = Command.ExecuteReader())
        {
            var results = new List<string>();
            while (reader.Read())
            {
                var text = reader.IsDBNull(1) ? null : reader.GetString(1);
                results.Add(text);
            }

            results.Should().BeEquivalentTo(words);
        }
    }

    // https://learn.microsoft.com/dotnet/api/system.data.common.dbcommand.executenonquery:
    // "For UPDATE, INSERT, and DELETE statements, the return value is the number of rows affected by the command.
    // For all other types of statements, the return value is -1."
    [Theory]
    [InlineData("SELECT 42")]
    [InlineData("CREATE TABLE minus_one (id INTEGER)")]
    [InlineData("SET threads = 1")]
    public async Task NonDmlStatementsReturnMinusOne(string sql)
    {
        Command.CommandText = sql;
        Command.ExecuteNonQuery().Should().Be(-1);

        Command.CommandText = sql.Replace("minus_one", "minus_one_async");
        (await Command.ExecuteNonQueryAsync()).Should().Be(-1);
    }

    [Fact]
    public void DmlStatementsReturnTheirRowCountIncludingZero()
    {
        Command.CommandText = "CREATE TABLE counted (id INTEGER); INSERT INTO counted VALUES (1), (2), (3);";
        Command.ExecuteNonQuery().Should().Be(3, "only the INSERT is counted");

        Command.CommandText = "UPDATE counted SET id = id + 10 WHERE id > 100";
        Command.ExecuteNonQuery().Should().Be(0);

        Command.CommandText = "DELETE FROM counted WHERE id = 2";
        Command.ExecuteNonQuery().Should().Be(1);
    }

    [Fact]
    public void MergeReportsItsRowCount()
    {
        Command.CommandText = "CREATE TABLE merge_counted (id INTEGER, name VARCHAR); INSERT INTO merge_counted VALUES (1, 'old');";
        Command.ExecuteNonQuery();

        Command.CommandText = "MERGE INTO merge_counted AS target USING (VALUES (1, 'updated'), (2, 'inserted')) AS source(id, name) ON target.id = source.id WHEN MATCHED THEN UPDATE SET name = source.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (source.id, source.name);";
        Command.ExecuteNonQuery().Should().Be(2);
    }

    [Fact]
    public void CopyReportsItsRowCount()
    {
        using var file = DisposableFile.GenerateInTemp("csv");
        Command.CommandText = "CREATE TABLE copy_counted (id INTEGER); INSERT INTO copy_counted VALUES (1), (2), (3);";
        Command.ExecuteNonQuery();

        Command.CommandText = $"COPY copy_counted TO '{file.FileName}' (FORMAT CSV);";
        Command.ExecuteNonQuery().Should().Be(3);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ReturningReportsItsRowCount(bool useStreamingMode)
    {
        var tableName = useStreamingMode ? "returning_counted_streaming" : "returning_counted_materialized";
        using var command = new DuckDBCommand
        {
            Connection = Connection,
            UseStreamingMode = useStreamingMode,
            CommandText = $"CREATE TABLE {tableName} (id INTEGER); INSERT INTO {tableName} VALUES (1), (2);"
        };
        command.ExecuteNonQuery();

        command.CommandText = $"INSERT INTO {tableName} VALUES (3), (4) RETURNING id;";
        command.ExecuteNonQuery().Should().Be(2);

        command.CommandText = $"UPDATE {tableName} SET id = id + 10 WHERE id <= 2 RETURNING id;";
        using var reader = command.ExecuteReader();
        while (reader.Read()) { }
        reader.Close();
        reader.RecordsAffected.Should().Be(2);
    }

    // https://learn.microsoft.com/dotnet/api/system.data.common.dbdatareader.recordsaffected:
    // "The number of rows changed, inserted, or deleted. -1 for SELECT statements; 0 if no rows were affected"
    [Fact]
    public void RecordsAffectedCountsDmlRunThroughExecuteReader()
    {
        Command.CommandText = "CREATE TABLE through_reader (id INTEGER); INSERT INTO through_reader VALUES (1), (2);";
        Command.ExecuteNonQuery();

        Command.CommandText = "DELETE FROM through_reader WHERE id = 2";
        using (var reader = Command.ExecuteReader())
        {
            reader.Close();
            reader.RecordsAffected.Should().Be(1);
        }

        Command.CommandText = "DELETE FROM through_reader WHERE id = 99";
        using (var reader = Command.ExecuteReader())
        {
            reader.Close();
            reader.RecordsAffected.Should().Be(0);
        }

        Command.CommandText = "SELECT id FROM through_reader";
        using (var reader = Command.ExecuteReader())
        {
            while (reader.Read()) { }
            reader.Close();
            reader.RecordsAffected.Should().Be(-1);
        }
    }
}
