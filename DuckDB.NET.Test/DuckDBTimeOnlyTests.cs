namespace DuckDB.NET.Test;

public class DuckDBTimeOnlyTests
{
    [Fact]
    public void TimeOnlyAsDateTimeIsTheTimeOnTheFirstDay()
    {
        new DuckDBTimeOnly(0, 0, 0).ToDateTime().Should().Be(DateTime.MinValue);
        new DuckDBTimeOnly(13, 14, 15, 123456).ToDateTime().Should().Be(new DateTime(1, 1, 1, 13, 14, 15).AddTicks(1234560));
        new DuckDBTimeOnly(23, 59, 59, 999999).ToDateTime().Should().Be(new DateTime(1, 1, 1, 23, 59, 59).AddTicks(9999990));
        ((DateTime)new DuckDBTimeOnly(1, 2, 3)).Should().Be(new DateTime(1, 1, 1, 1, 2, 3));
    }

    // DuckDB adds up the parts in int64 with no check (Time::FromTime), and the ticks must agree with it, also for a
    // microsecond count that an int times ten cannot hold. A TIME_NS write uses the ticks.
    [Theory]
    [InlineData(0, 0, 0, 214_748_365)]
    [InlineData(0, 0, 0, 300_000_000)]
    [InlineData(1, 2, 3, int.MaxValue)]
    [InlineData(23, 59, 59, -300_000_000)]
    [InlineData(0, 0, 0, int.MinValue)]
    public void TicksAgreeWithDuckDBForLargeMicroseconds(byte hour, byte minute, byte second, int microsecond)
    {
        var time = new DuckDBTimeOnly(hour, minute, second, microsecond);

        time.Ticks.Should().Be(NativeMethods.DateTimeHelpers.DuckDBToTime(time).Micros * TimeSpan.TicksPerMicrosecond);
    }

    [Fact]
    public void TimesWithATimeZoneAreEqualWhenTimeAndOffsetAre()
    {
        var time = new DuckDBTimeTz { Time = new DuckDBTimeOnly(13, 14, 15, 16), Offset = 3600 };

        time.Equals(new DuckDBTimeTz { Time = new DuckDBTimeOnly(13, 14, 15, 16), Offset = 3600 }).Should().BeTrue();
        time.Equals((object)new DuckDBTimeTz { Time = new DuckDBTimeOnly(13, 14, 15, 16), Offset = 3600 }).Should().BeTrue();
        time.GetHashCode().Should().Be(new DuckDBTimeTz { Time = new DuckDBTimeOnly(13, 14, 15, 16), Offset = 3600 }.GetHashCode());

        time.Equals(new DuckDBTimeTz { Time = new DuckDBTimeOnly(13, 14, 15, 17), Offset = 3600 }).Should().BeFalse();
        time.Equals(new DuckDBTimeTz { Time = new DuckDBTimeOnly(13, 14, 15, 16), Offset = 7200 }).Should().BeFalse();
        time.Equals("13:14:15").Should().BeFalse();
    }
}
