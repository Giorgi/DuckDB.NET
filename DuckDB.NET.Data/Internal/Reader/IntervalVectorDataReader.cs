using System;
using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Data.Internal.Reader;

internal class IntervalVectorDataReader : VectorDataReaderBase
{
    private static readonly Type TimeSpanType = typeof(TimeSpan);
    private static readonly Type TimeSpanNullableType = typeof(TimeSpan?);

    internal unsafe IntervalVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

    protected override T GetValidValue<T>(ulong offset, Type targetType)
    {
        if (DuckDBType == DuckDBType.Interval)
        {
            var interval = GetFieldData<DuckDBInterval>(offset);

            if (targetType == TimeSpanType || targetType == TimeSpanNullableType)
            {
                var timeSpan = (TimeSpan)interval;
                return (T)(object)timeSpan;
            }

            return (T)(object)interval;
        }

        return base.GetValidValue<T>(offset, targetType);
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

        if (targetType == TimeSpanType)
        {
            return (TimeSpan)interval;
        }

        return interval;
    }
}