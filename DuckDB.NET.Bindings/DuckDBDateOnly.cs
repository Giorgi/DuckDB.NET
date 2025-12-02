using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBDateOnly(int year, byte month, byte day)
{
    /// <summary>
    /// Represents positive infinity for DuckDB dates.
    /// </summary>
    public static readonly DuckDBDateOnly PositiveInfinity =
        // This is the value returned by DuckDB for positive infinity dates when
        // passed to duckdb_from_date, and it is used for backwards compatibility
        new(5881580, 7, 11);

    /// <summary>
    /// Represents negative infinity for DuckDB dates.
    /// </summary>
    public static readonly DuckDBDateOnly NegativeInfinity =
        // This is the value returned by DuckDB for negative infinity dates when
        // passed to duckdb_from_date, and it is used for backwards compatibility.
        new(-5877641, 6, 24);

    public int Year { get; } = year;

    public byte Month { get; } = month;

    public byte Day { get; } = day;

    internal static readonly DuckDBDateOnly MinValue = FromDateTime(DateTime.MinValue);

    /// <summary>
    /// Returns true if this date represents positive or negative infinity.
    /// </summary>
    public bool IsInfinity => IsPositiveInfinity || IsNegativeInfinity;

    /// <summary>
    /// Returns true if this date represents positive infinity.
    /// </summary>
    public bool IsPositiveInfinity => Equals(PositiveInfinity);

    /// <summary>
    /// Returns true if this date represents negative infinity.
    /// </summary>
    public bool IsNegativeInfinity => Equals(NegativeInfinity);

    public static DuckDBDateOnly FromDateTime(DateTime dateTime) => new DuckDBDateOnly(dateTime.Year, (byte)dateTime.Month, (byte)dateTime.Day);

    public DateTime ToDateTime() => new DateTime(Year, Month, Day);

#if NET6_0_OR_GREATER

    public static DuckDBDateOnly FromDateOnly(DateOnly dateOnly) => new DuckDBDateOnly(dateOnly.Year, (byte)dateOnly.Month, (byte)dateOnly.Day);

    public DateOnly ToDateOnly() => new DateOnly(Year, Month, Day);

#endif

    /// <summary>
    /// Converts a DuckDBDate to DuckDBDateOnly, handling infinity values.
    /// </summary>
    public static DuckDBDateOnly FromDuckDBDate(DuckDBDate date)
    {
        if (date.IsPositiveInfinity)
            return PositiveInfinity;
        if (date.IsNegativeInfinity)
            return NegativeInfinity;

        return NativeMethods.DateTimeHelpers.DuckDBFromDate(date);
    }

    /// <summary>
    /// Converts this DuckDBDateOnly to a DuckDBDate, handling infinity values.
    /// </summary>
    public DuckDBDate ToDuckDBDate()
    {
        if (IsPositiveInfinity)
            return DuckDBDate.PositiveInfinity;
        if (IsNegativeInfinity)
            return DuckDBDate.NegativeInfinity;

        return NativeMethods.DateTimeHelpers.DuckDBToDate(this);
    }

    public static explicit operator DateTime(DuckDBDateOnly dateOnly) => dateOnly.ToDateTime();

    public static explicit operator DuckDBDateOnly(DateTime dateTime) => FromDateTime(dateTime);

#if NET6_0_OR_GREATER

    public static implicit operator DateOnly(DuckDBDateOnly dateOnly) => dateOnly.ToDateOnly();

    public static implicit operator DuckDBDateOnly(DateOnly date) => DuckDBDateOnly.FromDateOnly(date);

#endif
}
