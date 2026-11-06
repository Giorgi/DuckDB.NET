using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Writer;

// A TIME column (DuckDBTime: microseconds from midnight) or a TIME_NS column (DuckDBTimeNs: nanoseconds), the mirror
// of TimeVectorDataReader. One closed type per column type, so the unit is a constant for the JIT: the units differ
// only in Store. A TimeOnly, a DuckDBTimeOnly and a DateTime (its time of day) are its only types; a DateOnly, which
// the writer of the dates and timestamps used to store here as the days of the date, is refused.
internal sealed unsafe class TimeVectorDataWriter<TStorage>(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
    where TStorage : unmanaged
{
    internal override bool AppendTimeOnly(TimeOnly value, ulong rowIndex) => Store(value.Ticks, rowIndex);

    internal override bool AppendTimeOnly(DuckDBTimeOnly value, ulong rowIndex) => Store(value.Ticks, rowIndex);

    // The time of day, as a TIME parameter takes it and as a TIME read as DateTime gives it back.
    internal override bool AppendDateTime(DateTime value, ulong rowIndex) => Store(value.Ticks % TimeSpan.TicksPerDay, rowIndex);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool Store(long ticksFromMidnight, ulong rowIndex)
    {
        if (typeof(TStorage) == typeof(DuckDBTimeNs))
        {
            return AppendValueInternal(DuckDBTimeNs.FromTicks(ticksFromMidnight), rowIndex);
        }

        return AppendValueInternal(DateTimeExtensions.ToDuckDBTime(ticksFromMidnight), rowIndex);
    }
}
