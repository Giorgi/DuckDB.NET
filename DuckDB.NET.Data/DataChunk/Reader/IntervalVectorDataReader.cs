namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class IntervalVectorDataReader : VectorDataReaderBase
{
    internal unsafe IntervalVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

    protected override T GetValidValue<T>(ulong offset)
    {
        if (DuckDBType == DuckDBType.Interval)
        {
            var interval = GetFieldData<DuckDBInterval>(offset);

            if (typeof(T) == typeof(TimeSpan))
            {
                var timeSpan = (TimeSpan)interval;
                return (T)(object)timeSpan;
            }

            return (T)(object)interval;
        }

        return base.GetValidValue<T>(offset);
    }

    internal override object GetValue(ulong offset, Type targetType)
    {
        return DuckDBType switch
        {
            DuckDBType.Interval => GetInterval(offset, targetType),
            _ => base.GetValue(offset, targetType)
        };
    }

    private object GetInterval(ulong offset, Type targetType)
    {
        var interval = GetFieldData<DuckDBInterval>(offset);

        if (targetType == typeof(TimeSpan))
        {
            return (TimeSpan)interval;
        }

        return interval;
    }
}