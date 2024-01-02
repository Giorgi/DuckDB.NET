using DuckDB.NET.Data.Extensions;
using System;

namespace DuckDB.NET.Data.Internal.Reader;

internal class BooleanVectorDataReader : VectorDataReaderBase
{
    internal unsafe BooleanVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

    internal override T GetValue<T>(ulong offset)
    {
        if (DuckDBType != DuckDBType.Boolean)
        {
            return base.GetValue<T>(offset);
        }

        if (IsValid(offset))
        {
            var value = GetFieldData<bool>(offset);
            return (T)(object)value; //JIT will optimize the casts at least for not nullable T
        }

        var (isNullable, _) = TypeExtensions.IsNullableValueType<T>();
        if (isNullable)
        {
            return default!;
        }

        throw new InvalidCastException($"Column '{ColumnName}' value is null");
    }

    internal override object GetValue(ulong offset, Type targetType)
    {
        return DuckDBType == DuckDBType.Boolean ? GetFieldData<bool>(offset) : base.GetValue(offset, targetType);
    }
}