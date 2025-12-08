using System;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBTimestamp(DuckDBDateOnly date, DuckDBTimeOnly time)
{
    /// <summary>
    /// Represents positive infinity for DuckDB timestamps.
    /// </summary>
    public static readonly DuckDBTimestamp PositiveInfinity =
        // This is the max timestamp value + 1 microsecond (because timestamps are represented as an int64 of microseconds)
        // Theoretically: '294247-01-10 04:00:54.775806'::timestamp + INTERVAL '1 microsecond'
        new(new DuckDBDateOnly(294247, 1, 10), new DuckDBTimeOnly(4, 0, 54, 775807));

    /// <summary>
    /// Represents negative infinity for DuckDB timestamps.
    /// </summary>
    public static readonly DuckDBTimestamp NegativeInfinity =
        // This is the min timestamp value - 1 microsecond (because timestamps are represented as an int64 of microseconds)
        // Theoretically: '290309-12-22 (BC) 00:00:00.000000'::timestamp - INTERVAL '1 microsecond'
        new(new DuckDBDateOnly(-290308, 12, 21), new DuckDBTimeOnly(23, 59, 59, 999999));

    public DuckDBDateOnly Date { get; } = date;
    public DuckDBTimeOnly Time { get; } = time;

    /// <summary>
    /// Returns true if this timestamp represents positive or negative infinity.
    /// </summary>
    public bool IsInfinity => IsPositiveInfinity || IsNegativeInfinity;

    /// <summary>
    /// Returns true if this timestamp represents positive infinity.
    /// </summary>
    public bool IsPositiveInfinity => Equals(PositiveInfinity);

    /// <summary>
    /// Returns true if this timestamp represents negative infinity.
    /// </summary>
    public bool IsNegativeInfinity => Equals(NegativeInfinity);

    public DateTime ToDateTime()
    {
        return new DateTime(Date.Year, Date.Month, Date.Day).AddTicks(Time.Ticks);
    }

    public static DuckDBTimestamp FromDateTime(DateTime dateTime)
    {
        return new DuckDBTimestamp(DuckDBDateOnly.FromDateTime(dateTime), DuckDBTimeOnly.FromDateTime(dateTime));
    }

    /// <summary>
    /// Converts a DuckDBTimestampStruct to DuckDBTimestamp, handling infinity values.
    /// </summary>
    public static DuckDBTimestamp FromDuckDBTimestampStruct(DuckDBTimestampStruct timestampStruct)
    {
        if (timestampStruct.IsPositiveInfinity)
            return PositiveInfinity;
        if (timestampStruct.IsNegativeInfinity)
            return NegativeInfinity;

        return NativeMethods.DateTimeHelpers.DuckDBFromTimestamp(timestampStruct);
    }

    /// <summary>
    /// Converts this DuckDBTimestamp to a DuckDBTimestampStruct, handling infinity values.
    /// </summary>
    public DuckDBTimestampStruct ToDuckDBTimestampStruct()
    {
        if (IsPositiveInfinity)
            return DuckDBTimestampStruct.PositiveInfinity;
        if (IsNegativeInfinity)
            return DuckDBTimestampStruct.NegativeInfinity;

        return NativeMethods.DateTimeHelpers.DuckDBToTimestamp(this);
    }

    public static implicit operator DateTime(DuckDBTimestamp timestamp) => timestamp.ToDateTime();
    public static implicit operator DuckDBTimestamp(DateTime timestamp) => DuckDBTimestamp.FromDateTime(timestamp);
}
