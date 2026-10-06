using System.Data.Common;
using System.Diagnostics;
using DuckDB.NET.Test.Helpers;

namespace DuckDB.NET.Test;

public class DuckDBIntervalTests
{
    [Theory]
    [InlineData(10, 0, "10.00:00:00")]
    [InlineData(0, 17, "00:00:00.000017")]
    [InlineData(13, 17, "13.00:00:00.000017")]
    [InlineData(13, 60e6, "13.00:01:00")]
    [InlineData(13, 3600e6, "13.01:00:00")]
    [InlineData(13, 24 * 3600e6, "14.00:00:00")]
    [InlineData(0, 24 * 3600e6 + 15 * 3600e6 + 60e6, "1.15:01:00")]
    [InlineData(13, 24 * 3600e6 + 60e6, "14.00:01:00")]
    public void ToTimeSpan_ValidValue_ExpectedResult(int days, ulong micros, string ts)
    {
        var interval = new DuckDBInterval(0, days, micros);
        Assert.True(interval.TryConvert(out var timeSpan));
        Assert.Equal(TimeSpan.Parse(ts), timeSpan);
        Assert.Equal(TimeSpan.Parse(ts), (TimeSpan)interval);
    }

    [Fact]
    public void ToTimeSpan_MonthInterval_Exception()
    {
        var interval = new DuckDBInterval(1, 0, 0);
        Assert.False(interval.TryConvert(out var ts));
        Assert.Throws<ArgumentOutOfRangeException>(() => (TimeSpan)interval);
    }

    [Fact]
    public void ToTimeSpan_TooBigMicros_Exception()
    {
        var interval = new DuckDBInterval(0, 0, ((ulong)long.MaxValue) + 1);
        Assert.False(interval.TryConvert(out var ts));
        Assert.Throws<ArgumentOutOfRangeException>(() => (TimeSpan)interval);
    }

    [Fact]
    public void ToTimeSpan_TooBigDays_Exception()
    {
        var interval = new DuckDBInterval(0, int.MaxValue, (ulong)24 * 60 * 60 * 1000 * 1000);
        Assert.False(interval.TryConvert(out var ts));
        Assert.Throws<ArgumentOutOfRangeException>(() => (TimeSpan)interval);
    }

    // DuckDB stores an interval's micros as a signed 64-bit value (duckdb_interval.micros is int64_t).
    [Theory]
    [InlineData(0, -5_400_000_000L, "-01:30:00")]
    [InlineData(0, -17L, "-00:00:00.000017")]
    [InlineData(-1, -3_600_000_000L, "-1.01:00:00")]
    [InlineData(1, -3_600_000_000L, "23:00:00")]
    [InlineData(-2, 0L, "-2.00:00:00")]
    [InlineData(-1, 25 * 3_600_000_000L, "01:00:00")]
    public void ToTimeSpan_NegativeValue_ExpectedResult(int days, long micros, string ts)
    {
        var interval = new DuckDBInterval(0, days, unchecked((ulong)micros));
        Assert.True(interval.TryConvert(out var timeSpan));
        Assert.Equal(TimeSpan.Parse(ts), timeSpan);
        Assert.Equal(TimeSpan.Parse(ts), (TimeSpan)interval);
    }

    [Fact]
    public void ToTimeSpan_NegativeMonthInterval_Exception()
    {
        var interval = new DuckDBInterval(-1, 0, 0);
        Assert.False(interval.TryConvert(out var ts));
        Assert.Throws<ArgumentOutOfRangeException>(() => (TimeSpan)interval);
    }

    [Fact]
    public void ToTimeSpan_TooSmall_Exception()
    {
        var interval = new DuckDBInterval(0, int.MinValue, unchecked((ulong)long.MinValue));
        Assert.False(interval.TryConvert(out var ts));
        Assert.Throws<ArgumentOutOfRangeException>(() => (TimeSpan)interval);
    }

    [Theory]
    [InlineData("-01:30:00", 0, -5_400_000_000L)]
    [InlineData("-1.01:00:00", -1, -3_600_000_000L)]
    [InlineData("-00:00:00.0000017", 0, -1L)]
    public void ToDuckDBInterval_NegativeValue_ExpectedResult(string ts, int days, long micros)
    {
        DuckDBInterval interval = TimeSpan.Parse(ts);
        Assert.Equal(0, interval.Months);
        Assert.Equal(days, interval.Days);
        Assert.Equal(micros, unchecked((long)interval.Micros));
    }

    [Fact]
    public void ToDuckDBInterval_ValidValue_ExpectedResult()
    {
        DuckDBInterval ts = new TimeSpan(5, 17, 12, 45);
        Assert.Equal(0, ts.Months);
        Assert.Equal(5, ts.Days);
        Assert.Equal(((17 * 60 + 12) * 60 + 45) * (ulong)1e6, ts.Micros);
    }
}