namespace DuckDB.NET.Test;

// DuckDBDateOnly and DuckDBTimestamp convert in C# (DuckDBTimeConversions), with the results of V1's duckdb_from_date,
// duckdb_to_date, duckdb_from_timestamp and duckdb_to_timestamp, which these tests compare them with.
public class DuckDBDateConversionTests
{
    [Fact]
    public void DatesConvertAsDuckDBConvertsThem()
    {
        var random = new Random(20261125);
        var days = Enumerable.Range(-800_000, 1_600_001)                                              // about 2,200 years either side of 1970
                             .Concat(Enumerable.Range(0, 500_000).Select(_ => random.Next(-2_147_483_646, 2_147_483_647)))
                             .Concat([-2_147_483_646, -2_147_483_645, 2_147_483_645, 2_147_483_646, -719_468, -719_469, -719_162]);

        foreach (var day in days)
        {
            var expected = NativeMethods.DateTimeHelpers.DuckDBFromDate(new DuckDBDate { Days = day });
            var date = DuckDBTimeConversions.FromDate(day);

            // Compared without FluentAssertions: there are 2.1 million days.
            if (!date.Equals(expected) || DuckDBTimeConversions.ToDate(date) != day)
            {
                Assert.Fail($"Day {day}: DuckDB gives {expected.Year}-{expected.Month}-{expected.Day}, the conversion {date.Year}-{date.Month}-{date.Day}, " +
                            $"and back {DuckDBTimeConversions.ToDate(date)}");
            }
        }
    }

    [Theory]
    [InlineData(2024, 2, 30)]
    [InlineData(2023, 2, 29)]
    [InlineData(1900, 2, 29)]
    [InlineData(-1, 2, 29)]
    [InlineData(2024, 13, 1)]
    [InlineData(2024, 0, 1)]
    [InlineData(2024, 1, 0)]
    [InlineData(2024, 4, 31)]
    [InlineData(5881580, 7, 11)]
    [InlineData(5881581, 1, 1)]
    [InlineData(-5877641, 6, 24)]
    [InlineData(-5877642, 1, 1)]
    [InlineData(2000, 2, 29)]
    [InlineData(0, 2, 29)]
    [InlineData(-4, 2, 29)]
    [InlineData(5881580, 7, 10)]
    [InlineData(-5877641, 6, 25)]
    public void ADateThatDoesNotExistOrIsOutOfRangeIsDayZeroAsInDuckDB(int year, int month, int day)
    {
        var date = new DuckDBDateOnly(year, (byte)month, (byte)day);

        DuckDBTimeConversions.ToDate(date).Should().Be(NativeMethods.DateTimeHelpers.DuckDBToDate(date).Days);
    }

    [Fact]
    public void TimestampsConvertAsDuckDBConvertsThem()
    {
        var random = new Random(20261125);
        var micros = Enumerable.Range(0, 1_000_000).Select(_ => random.NextInt64(long.MinValue, long.MaxValue))
                               .Concat(Enumerable.Range(-200_000, 400_001).Select(step => step * 999_983L))  // both sides of 1970
                               .Concat([0, -1, 1, -86_400_000_000, -86_400_000_001, 86_399_999_999,
                                        long.MinValue, long.MinValue + 1, long.MinValue + 2, long.MaxValue - 1, long.MaxValue]);

        foreach (var value in micros)
        {
            var expected = NativeMethods.DateTimeHelpers.DuckDBFromTimestamp(new DuckDBTimestampStruct { Micros = value });
            var timestamp = DuckDBTimeConversions.FromTimestamp(value);
            var back = DuckDBTimeConversions.ToTimestamp(timestamp);
            var expectedBack = NativeMethods.DateTimeHelpers.DuckDBToTimestamp(expected).Micros;

            if (!timestamp.Equals(expected) || back != expectedBack)
            {
                Assert.Fail($"{value}: DuckDB gives {Text(expected)} and back {expectedBack}, the conversion {Text(timestamp)} and back {back}");
            }
        }

        static string Text(DuckDBTimestamp t) => $"{t.Date.Year}-{t.Date.Month}-{t.Date.Day} {t.Time.Hour}:{t.Time.Min}:{t.Time.Sec}.{t.Time.Microsecond}";
    }

    [Theory]
    [InlineData(2024, 2, 30, 1, 2, 3, 4)]                 // a date that does not exist
    [InlineData(2024, 2, 28, 25, 0, 0, 0)]                // a time past the day: DuckDB adds it up
    [InlineData(2024, 2, 28, 23, 60, 0, 0)]
    [InlineData(2024, 2, 28, 23, 0, 0, 1_000_000)]
    [InlineData(294247, 1, 10, 4, 0, 54, 775806)]         // the last TIMESTAMP
    [InlineData(294247, 1, 10, 4, 0, 54, 775807)]         // positive infinity
    [InlineData(294247, 1, 11, 0, 0, 0, 0)]               // past it
    [InlineData(-290308, 12, 22, 0, 0, 0, 0)]
    [InlineData(-290308, 12, 21, 23, 59, 59, 999999)]     // the day's midnight overflows, its last microsecond does not
    [InlineData(-290308, 12, 21, 0, 0, 0, 0)]
    [InlineData(300000, 1, 1, 0, 0, 0, 0)]
    public void ATimestampIsPutTogetherAsDuckDBPutsItTogether(int year, int month, int day, int hour, int minute, int second, int microsecond)
    {
        var timestamp = new DuckDBTimestamp(new DuckDBDateOnly(year, (byte)month, (byte)day), new DuckDBTimeOnly((byte)hour, (byte)minute, (byte)second, microsecond));

        DuckDBTimeConversions.ToTimestamp(timestamp).Should().Be(NativeMethods.DateTimeHelpers.DuckDBToTimestamp(timestamp).Micros);
    }
}
