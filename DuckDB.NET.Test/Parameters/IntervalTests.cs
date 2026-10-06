namespace DuckDB.NET.Test.Parameters;

// DuckDB intervals can be negative: duckdb_interval.micros is a signed 64-bit value.
public class IntervalTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Theory]
    [InlineData("-90 minutes", -90 * 60)]
    [InlineData("-1 day", -86_400)]
    [InlineData("-1 day 2 hours", -79_200)]
    [InlineData("1 day -2 hours", 79_200)]
    public void QueryNegativeInterval(string interval, int seconds)
    {
        Command.CommandText = $"SELECT INTERVAL '{interval}';";

        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetValue(0).Should().Be(TimeSpan.FromSeconds(seconds));
        reader.GetFieldValue<TimeSpan>(0).Should().Be(TimeSpan.FromSeconds(seconds));
    }

    [Theory]
    [InlineData("-1 month")]
    [InlineData("1 month")]
    public void QueryMonthInterval_AsTimeSpan_Throws(string interval)
    {
        Command.CommandText = $"SELECT INTERVAL '{interval}';";

        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetFieldValue<TimeSpan>(0)).Should().Throw<ArgumentOutOfRangeException>();
        reader.GetFieldValue<DuckDBInterval>(0).Months.Should().Be(interval.StartsWith('-') ? -1 : 1);
    }

    [Theory]
    [InlineData(-5_400_000_000L)]
    [InlineData(-1L)]
    [InlineData(-90_061_000_001L)]
    public void InsertNegativeTimeSpanParameter(long micros)
    {
        var expected = TimeSpan.FromTicks(micros * 10);

        Command.CommandText = "CREATE OR REPLACE TABLE NegativeIntervalParameterTable (v INTERVAL);";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO NegativeIntervalParameterTable (v) VALUES (?);";
        Command.Parameters.Add(new DuckDBParameter(expected));
        Command.ExecuteNonQuery();
        Command.Parameters.Clear();

        Command.CommandText = $"SELECT v, v = CAST('{micros} microseconds' AS INTERVAL) FROM NegativeIntervalParameterTable;";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<TimeSpan>(0).Should().Be(expected);
        reader.GetBoolean(1).Should().BeTrue();
    }

    [Fact]
    public void AppendNegativeTimeSpan()
    {
        Command.CommandText = "CREATE TABLE NegativeIntervalTable (v INTERVAL);";
        Command.ExecuteNonQuery();
        var expected = TimeSpan.FromMinutes(-90).Add(TimeSpan.FromTicks(-10));

        using (var appender = Connection.CreateAppender("NegativeIntervalTable"))
        {
            appender.CreateRow().AppendValue((TimeSpan?)expected).EndRow();
        }

        Command.CommandText = "SELECT v, v = INTERVAL '-5400000001 microseconds' FROM NegativeIntervalTable;";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<TimeSpan>(0).Should().Be(expected);
        reader.GetBoolean(1).Should().BeTrue();
    }
}
