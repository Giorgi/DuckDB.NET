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

    // A TimeSpan has no DbType, so where DuckDB can't infer the parameter's type (a bare "?") it was
    // sent as TimeSpan.ToString(), which DuckDB can't parse as an interval once there are days
    // ("1.01:01:01"), and which came back as a string.
    [Theory]
    [InlineData(90_061_000_001L)]
    [InlineData(-90_061_000_001L)]
    [InlineData(5_400_000_000L)]
    public void BindTimeSpanWithoutTargetType(long micros)
    {
        var expected = TimeSpan.FromTicks(micros * 10);

        Command.CommandText = "SELECT ?::INTERVAL, ?;";
        Command.Parameters.Add(new DuckDBParameter(expected));
        Command.Parameters.Add(new DuckDBParameter(expected));

        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<TimeSpan>(0).Should().Be(expected);
        reader.GetValue(1).Should().Be(expected);
    }

    // A TimeSpan bound where DuckDB knows the parameter's type keeps its existing behavior.
    [Fact]
    public void BindTimeSpanIntoTimeAndVarcharColumns()
    {
        Command.CommandText = "CREATE OR REPLACE TABLE TimeSpanTargets (t TIME, s VARCHAR);";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO TimeSpanTargets (t, s) VALUES (?, ?);";
        Command.Parameters.Add(new DuckDBParameter(new TimeSpan(0, 13, 45, 30, 125)));
        Command.Parameters.Add(new DuckDBParameter(new TimeSpan(0, 1, 30, 0)));
        Command.ExecuteNonQuery();
        Command.Parameters.Clear();

        Command.CommandText = "SELECT t, s FROM TimeSpanTargets;";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<TimeOnly>(0).Should().Be(new TimeOnly(13, 45, 30, 125));
        reader.GetString(1).Should().Be("01:30:00");
    }

    // As in Npgsql, a caller who wants a TimeSpan sent as a time declares DbType.Time: where DuckDB can't
    // infer the type, an interval has no cast to TIME.
    [Theory]
    [InlineData(0, 0, 0, 0)]
    [InlineData(13, 45, 30, 125)]
    [InlineData(23, 59, 59, 999)]
    public void InsertSelectTimeSpanDeclaredTime(int hours, int minutes, int seconds, int milliseconds)
    {
        var value = new TimeSpan(0, hours, minutes, seconds, milliseconds);

        Command.CommandText = "CREATE OR REPLACE TABLE TimeSpanDeclaredTime (t TIME);";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO TimeSpanDeclaredTime (t) SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(DbType.Time, value));
        Command.ExecuteNonQuery();
        Command.Parameters.Clear();

        Command.CommandText = "SELECT t FROM TimeSpanDeclaredTime;";
        Command.ExecuteScalar().Should().Be(TimeOnly.FromTimeSpan(value));
    }

    [Fact]
    public void SelectTimeSpanDeclaredTime()
    {
        Command.CommandText = "SELECT ?::TIME::VARCHAR, ? UNION ALL SELECT '', TIME '01:00';";
        Command.Parameters.Add(new DuckDBParameter(DbType.Time, new TimeSpan(0, 1, 30, 0)));
        Command.Parameters.Add(new DuckDBParameter(DbType.Time, new TimeSpan(0, 2, 0, 0)));

        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetString(0).Should().Be("01:30:00");
        reader.GetFieldValue<TimeOnly>(1).Should().Be(new TimeOnly(2, 0));
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(0, -1)]
    public void TimeSpanDeclaredTime_OutOfRange_Throws(int days, int seconds)
    {
        var value = new TimeSpan(days, 0, 0, seconds);

        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(DbType.Time, value));

        Command.Invoking(c => c.ExecuteScalar()).Should().Throw<ArgumentOutOfRangeException>();
    }
}
