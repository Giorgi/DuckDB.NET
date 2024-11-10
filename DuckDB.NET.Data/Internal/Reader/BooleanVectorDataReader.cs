using DuckDB.NET.Data.Extensions;
using System;
using DuckDB.NET.Native;
using System.Diagnostics.CodeAnalysis;

namespace DuckDB.NET.Data.Internal.Reader;

internal sealed class BooleanVectorDataReader : VectorDataReaderBase
{
    internal unsafe BooleanVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

#if NET8_0_OR_GREATER
    protected override T GetValidValue<T>(ulong offset, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType)
#else
    protected override T GetValidValue<T>(ulong offset, Type targetType) 
#endif
    {
        if (DuckDBType != DuckDBType.Boolean)
        {
            return base.GetValidValue<T>(offset, targetType);
        }
        
        var value = GetFieldData<bool>(offset);
        return (T)(object)value; //JIT will optimize the casts at least for not nullable T
    }

#if NET8_0_OR_GREATER
    internal override object GetValue(ulong offset, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType)
#else
    internal override object GetValue(ulong offset, Type targetType)
#endif
    {
        if (DuckDBType != DuckDBType.Boolean)
        {
            return base.GetValue(offset, targetType);
        }

        return GetFieldData<bool>(offset);
    }
}