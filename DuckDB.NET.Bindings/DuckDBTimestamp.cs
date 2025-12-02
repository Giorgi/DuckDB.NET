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
        // The +infinity date value is not representable by the timestamp type,
        // so this constant should never occur in normal usage
        new(DuckDBDateOnly.PositiveInfinity, new DuckDBTimeOnly(0, 0, 0));

    /// <summary>
    /// Represents negative infinity for DuckDB timestamps.
    /// </summary>
    public static readonly DuckDBTimestamp NegativeInfinity =
        // The -infinity date value is not representable by the timestamp type,
        // so this constant should never occur in normal usage
        new(DuckDBDateOnly.NegativeInfinity, new DuckDBTimeOnly(0, 0, 0));

    public DuckDBDateOnly Date { get; } = date;
    public DuckDBTimeOnly Time { get; } = time;

    /// <summary>
    /// Returns true if this timestamp represents positive or negative infinity.
    /// </summary>
    public bool IsInfinity => Date.IsInfinity;

    /// <summary>
    /// Returns true if this timestamp represents positive infinity.
    /// </summary>
    public bool IsPositiveInfinity => Date.IsPositiveInfinity;

    /// <summary>
    /// Returns true if this timestamp represents negative infinity.
    /// </summary>
    public bool IsNegativeInfinity => Date.IsNegativeInfinity;

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
