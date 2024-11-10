using System;
using System.Diagnostics.CodeAnalysis;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Reader;

internal sealed class IntervalVectorDataReader : VectorDataReaderBase
{
    private static readonly Type TimeSpanType = typeof(TimeSpan);
    private static readonly Type TimeSpanNullableType = typeof(TimeSpan?);

    internal unsafe IntervalVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

#if NET8_0_OR_GREATER
    protected override T GetValidValue<T>(ulong offset, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType)
#else
    protected override T GetValidValue<T>(ulong offset, Type targetType)
#endif
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

#if NET8_0_OR_GREATER
    internal override object GetValue(ulong offset, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType)
#else
    internal override object GetValue(ulong offset, Type targetType)
#endif
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