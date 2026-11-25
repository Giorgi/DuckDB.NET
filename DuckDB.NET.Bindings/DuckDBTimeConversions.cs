using System.Runtime.CompilerServices;

namespace DuckDB.NET.Native;

/// <summary>
/// DuckDB's conversions of DATE, TIME, TIMETZ and TIMESTAMP, in C#: the arithmetic that V1's <c>duckdb_from_date</c>,
/// <c>duckdb_to_date</c>, <c>duckdb_from_time</c>, <c>duckdb_from_time_tz</c>, <c>duckdb_from_timestamp</c> and
/// <c>duckdb_to_timestamp</c> run. The C API V2 has no such calls, and the arithmetic saves a native call per value.
/// </summary>
internal static class DuckDBTimeConversions
{
    private const long MicrosPerSecond = 1_000_000;
    private const long MicrosPerMinute = 60 * MicrosPerSecond;
    private const long MicrosPerHour = 60 * MicrosPerMinute;
    private const long MicrosPerDay = 24 * MicrosPerHour;

    // A TIMETZ holds the microseconds since midnight in the upper 40 bits, and in the lower 24 bits the offset in
    // seconds subtracted from MaxOffset, so that the bits sort by instant.
    private const int OffsetBits = 24;
    private const ulong OffsetMask = (1UL << OffsetBits) - 1;
    private const int MaxOffset = 16 * 60 * 60 - 1;

    /// <summary>A TIME, microseconds since midnight, as hours, minutes, seconds and microseconds.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DuckDBTimeOnly FromTime(long micros)
    {
        var hour = micros / MicrosPerHour;
        micros -= hour * MicrosPerHour;
        var minute = micros / MicrosPerMinute;
        micros -= minute * MicrosPerMinute;
        var second = micros / MicrosPerSecond;
        micros -= second * MicrosPerSecond;

        return new DuckDBTimeOnly((byte)hour, (byte)minute, (byte)second, (int)micros);
    }

    /// <summary>
    /// A DATE, days since 1970-01-01, as year, month and day of the proleptic Gregorian calendar. Years are numbered
    /// astronomically, as DuckDB numbers them: year 0 is 1 BC.
    /// </summary>
    /// <remarks>
    /// Howard Hinnant's <c>civil_from_days</c> (https://howardhinnant.github.io/date_algorithms.html): days are counted
    /// from 0000-03-01, so that the leap day ends a year, and split into eras of 400 years, each 146,097 days.
    /// </remarks>
    public static DuckDBDateOnly FromDate(int days)
    {
        long z = days + 719_468L;
        var era = (z >= 0 ? z : z - 146_096) / 146_097;
        var dayOfEra = z - era * 146_097;                                                            // [0, 146096]
        var yearOfEra = (dayOfEra - dayOfEra / 1_460 + dayOfEra / 36_524 - dayOfEra / 146_096) / 365; // [0, 399]
        var dayOfYear = dayOfEra - (365 * yearOfEra + yearOfEra / 4 - yearOfEra / 100);                // [0, 365]
        var monthFromMarch = (5 * dayOfYear + 2) / 153;                                               // [0, 11]
        var day = dayOfYear - (153 * monthFromMarch + 2) / 5 + 1;                                     // [1, 31]
        var month = monthFromMarch < 10 ? monthFromMarch + 3 : monthFromMarch - 9;                    // [1, 12]
        var year = yearOfEra + era * 400 + (month <= 2 ? 1 : 0);

        return new DuckDBDateOnly((int)year, (byte)month, (byte)day);
    }

    // The range of a DATE: one day more or less is DuckDB's positive or negative infinity.
    private const int MinDate = -2_147_483_646;
    private const int MaxDate = 2_147_483_646;

    /// <summary>
    /// A date as a DATE, days since 1970-01-01: Hinnant's <c>days_from_civil</c>, the inverse of <see cref="FromDate"/>.
    /// As with V1's <c>duckdb_to_date</c>, a date that does not exist (a 30 February, a month 13) or is outside the range
    /// of a DATE gives 0, which is 1970-01-01.
    /// </summary>
    public static int ToDate(DuckDBDateOnly date)
    {
        if (!Exists(date))
        {
            return 0;
        }

        var days = DaysFromCivil(date);
        return days is < MinDate or > MaxDate ? 0 : (int)days;
    }

    /// <summary>A TIMESTAMP, microseconds since 1970-01-01 00:00, as its date and its time of day.</summary>
    public static DuckDBTimestamp FromTimestamp(long micros)
    {
        // The two infinities come out as the infinite dates at midnight, as from V1's duckdb_from_timestamp.
        if (micros is long.MaxValue or -long.MaxValue)
        {
            return new DuckDBTimestamp(micros > 0 ? DuckDBDateOnly.PositiveInfinity : DuckDBDateOnly.NegativeInfinity, new DuckDBTimeOnly(0, 0, 0, 0));
        }

        // The day is rounded down, so the time of a timestamp before 1970 is still counted forward from its midnight.
        var days = Math.DivRem(micros, MicrosPerDay, out var time);
        if (time < 0)
        {
            days--;
            time += MicrosPerDay;
        }

        return new DuckDBTimestamp(FromDate((int)days), FromTime(time));
    }

    /// <summary>
    /// A date and a time of day as a TIMESTAMP, microseconds since 1970-01-01 00:00. As with V1's
    /// <c>duckdb_to_timestamp</c>, a date that does not exist, or a sum that does not fit a TIMESTAMP or is one of its two
    /// infinities, gives 0; the time of day is not checked (25:00 is a day and an hour).
    /// </summary>
    public static long ToTimestamp(DuckDBTimestamp timestamp)
    {
        if (!Exists(timestamp.Date))
        {
            return 0;
        }

        var time = timestamp.Time;
        var micros = (Int128)DaysFromCivil(timestamp.Date) * MicrosPerDay +
                     time.Hour * MicrosPerHour + time.Min * MicrosPerMinute + time.Sec * MicrosPerSecond + time.Microsecond;

        return micros > long.MaxValue - 1 || micros < long.MinValue || micros == -long.MaxValue ? 0 : (long)micros;
    }

    private static long DaysFromCivil(DuckDBDateOnly date)
    {
        long year = date.Month <= 2 ? date.Year - 1L : date.Year;
        var era = (year >= 0 ? year : year - 399) / 400;
        var yearOfEra = year - era * 400;                                                            // [0, 399]
        var dayOfYear = (153 * (date.Month > 2 ? date.Month - 3 : date.Month + 9) + 2) / 5 + date.Day - 1;
        var dayOfEra = yearOfEra * 365 + yearOfEra / 4 - yearOfEra / 100 + dayOfYear;                  // [0, 146096]

        return era * 146_097 + dayOfEra - 719_468;
    }

    // Whether the date is in the proleptic Gregorian calendar, with astronomical years (year 0 is a leap year).
    private static bool Exists(DuckDBDateOnly date)
    {
        if (date.Month is < 1 or > 12 || date.Day < 1)
        {
            return false;
        }

        var leap = date.Year % 4 == 0 && (date.Year % 100 != 0 || date.Year % 400 == 0);
        int daysInMonth = date.Month switch
        {
            2 => leap ? 29 : 28,
            4 or 6 or 9 or 11 => 30,
            _ => 31,
        };

        return date.Day <= daysInMonth;
    }

    /// <summary>A TIMETZ: its time, and its offset from UTC in seconds.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DuckDBTimeTz FromTimeTz(ulong bits) => new()
    {
        Time = FromTime((long)(bits >> OffsetBits)),
        Offset = MaxOffset - (int)(bits & OffsetMask),
    };
}
