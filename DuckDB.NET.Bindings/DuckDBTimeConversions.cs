using System.Runtime.CompilerServices;

namespace DuckDB.NET.Native;

/// <summary>
/// DuckDB's conversions of TIME and TIMETZ, in C#: the arithmetic of <c>Time::Convert</c> and <c>dtime_tz_t</c>
/// (src/common/types/time.cpp, datetime.hpp), which V1's <c>duckdb_from_time</c> and <c>duckdb_from_time_tz</c> run.
/// The C API V2 has no such calls, and the arithmetic saves a native call per value.
/// </summary>
internal static class DuckDBTimeConversions
{
    private const long MicrosPerSecond = 1_000_000;
    private const long MicrosPerMinute = 60 * MicrosPerSecond;
    private const long MicrosPerHour = 60 * MicrosPerMinute;

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

    /// <summary>A TIMETZ: its time, and its offset from UTC in seconds.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DuckDBTimeTz FromTimeTz(ulong bits) => new()
    {
        Time = FromTime((long)(bits >> OffsetBits)),
        Offset = MaxOffset - (int)(bits & OffsetMask),
    };
}
