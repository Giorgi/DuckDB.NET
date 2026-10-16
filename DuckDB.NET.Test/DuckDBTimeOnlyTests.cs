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
