using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Reader;

// A TIME column (DuckDBTime: microseconds from midnight) or a TIME_NS column (DuckDBTimeNs: nanoseconds). One closed
// type per column type, as NumericVectorDataReader: the unit is a constant for the JIT, so a read has no test of the
// column type and no divisor kept in a field. The units differ only in GetTicks and GetDuckDBTimeOnly; every rule
// below is the same for both. StorageType is not set: the stored structs are not types a value is read as.
// TemporalConversionTests compares the .NET types with what DuckDB splits.
internal sealed class TimeVectorDataReader<TStorage> : VectorDataReaderBase where TStorage : unmanaged
{
    internal unsafe TimeVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

    internal override DateTime ReadDateTime(ulong offset) => GetValidValue<DateTime>(offset);
    internal override TimeOnly ReadTimeOnly(ulong offset) => GetValidValue<TimeOnly>(offset);

    protected override T GetValidValue<T>(ulong offset)
    {
        if (typeof(T) == typeof(TimeOnly))
        {
            return (T)(object)new TimeOnly(GetTicks(offset));
        }

        if (typeof(T) == typeof(DateTime))
        {
            return (T)(object)new DateTime(GetTicks(offset));
        }

        if (typeof(T) == typeof(TimeSpan))
        {
            return (T)(object)new TimeSpan(GetTicks(offset));
        }

        // DuckDBTimeOnly, and any other type, which the cast refuses.
        return (T)(object)GetDuckDBTimeOnly(offset);
    }

    internal override object GetValueCore(ulong offset, Type targetType)
    {
        if (targetType == typeof(TimeOnly))
        {
            return new TimeOnly(GetTicks(offset));
        }

        if (targetType == typeof(DateTime))
        {
            return new DateTime(GetTicks(offset));
        }

        if (targetType == typeof(TimeSpan))
        {
            return new TimeSpan(GetTicks(offset));
        }

        return GetDuckDBTimeOnly(offset);
    }

    // The ticks from midnight; what a TIME_NS has below a tick is dropped. DuckDB has 24:00:00 as a time; a TimeOnly
    // refuses it, a DateTime takes it as the start of the next day, and a TimeSpan as one day.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private long GetTicks(ulong offset)
    {
        var value = GetFieldData<TStorage>(offset);

        if (typeof(TStorage) == typeof(DuckDBTimeNs))
        {
            return Unsafe.As<TStorage, DuckDBTimeNs>(ref value).Ticks;
        }

        return Unsafe.As<TStorage, DuckDBTime>(ref value).Micros * TimeSpan.TicksPerMicrosecond;
    }

    // DuckDB splits the microseconds into hour, minute, second and microsecond; what a TIME_NS has below a
    // microsecond is dropped, as a TIMESTAMP_NS read as DuckDBTimestamp drops it.
    private DuckDBTimeOnly GetDuckDBTimeOnly(ulong offset)
    {
        var value = GetFieldData<TStorage>(offset);

        if (typeof(TStorage) == typeof(DuckDBTimeNs))
        {
            return Unsafe.As<TStorage, DuckDBTimeNs>(ref value).ToDuckDBTimeOnly();
        }

        return NativeMethods.DateTimeHelpers.DuckDBFromTime(Unsafe.As<TStorage, DuckDBTime>(ref value));
    }
}
