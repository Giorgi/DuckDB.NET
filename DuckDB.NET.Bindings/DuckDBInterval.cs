using System.Diagnostics.CodeAnalysis;

namespace DuckDB.NET.Native;

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBInterval(int months, int days, ulong micros) : IEquatable<DuckDBInterval>
{
    public int Months { get; } = months;

    public int Days { get; } = days;
    public ulong Micros { get; } = micros;

    public static explicit operator TimeSpan(DuckDBInterval interval)
    {
        var (timeSpan, exception) = ToTimeSpan(interval);
        return timeSpan ?? throw exception!;
    }
    public static implicit operator DuckDBInterval(TimeSpan timeSpan) => FromTimeSpan(timeSpan);

    public bool TryConvert([NotNullWhen(true)] out TimeSpan? timeSpan)
    {
        (timeSpan, var exception) = ToTimeSpan(this);
        return exception is null;
    }

    // DuckDB's micros is signed (duckdb_interval.micros is int64_t), and Days can be negative too. Micros
    // exposes the value as ulong, so it is read and written as its two's complement.
    private static (TimeSpan?, Exception?) ToTimeSpan(DuckDBInterval interval)
    {
        if (interval.Months != 0)
        {
            return (null, new ArgumentOutOfRangeException(nameof(interval), $"Cannot convert a value of type {nameof(DuckDBInterval)} to type {nameof(TimeSpan)} when the attribute 'Months' is not 0"));
        }

        var ticks = (Int128)interval.Days * TimeSpan.TicksPerDay + (Int128)unchecked((long)interval.Micros) * TimeSpan.TicksPerMicrosecond;
        if (ticks > long.MaxValue || ticks < long.MinValue)
        {
            return (null, new ArgumentOutOfRangeException(nameof(interval), $"Cannot convert a value of type {nameof(DuckDBInterval)} to type {nameof(TimeSpan)} when the total value is outside the range of {nameof(TimeSpan)}"));
        }

        return (new TimeSpan((long)ticks), null);
    }

    private static DuckDBInterval FromTimeSpan(TimeSpan timeSpan)
        => new(0, timeSpan.Days, unchecked((ulong)(timeSpan.Ticks % TimeSpan.TicksPerDay / TimeSpan.TicksPerMicrosecond)));

    // Without these, Equals is ValueType.Equals(object), which boxes. Two intervals are equal when their three
    // parts are: one month and thirty days are different values, as they are in the three fields DuckDB stores.
    public bool Equals(DuckDBInterval other) => Months == other.Months && Days == other.Days && Micros == other.Micros;

    public override bool Equals(object? obj) => obj is DuckDBInterval other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(Months, Days, Micros);
}