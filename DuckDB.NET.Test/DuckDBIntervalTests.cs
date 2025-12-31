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

    [Fact]
    public void ToDuckDBInterval_ValidValue_ExpectedResult()
    {
        DuckDBInterval ts = new TimeSpan(5, 17, 12, 45);
        Assert.Equal(0, ts.Months);
        Assert.Equal(5, ts.Days);
        Assert.Equal(((17 * 60 + 12) * 60 + 45) * (ulong)1e6, ts.Micros);
    }
}