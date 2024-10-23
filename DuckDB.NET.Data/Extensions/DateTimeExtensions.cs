using System;

namespace DuckDB.NET.Data.Extensions;

//https://stackoverflow.com/a/5359304/239438
internal static class DateTimeExtensions
{
    public const int TicksPerMicrosecond = 10;
    public const int NanosecondsPerTick = 100;

    public static int Nanoseconds(this DateTime self)
    {
#if NET8_0_OR_GREATER
        return self.Nanosecond;
#else
        return (int)(self.Ticks % TimeSpan.TicksPerMillisecond % TicksPerMicrosecond) * NanosecondsPerTick;
#endif
    }
}