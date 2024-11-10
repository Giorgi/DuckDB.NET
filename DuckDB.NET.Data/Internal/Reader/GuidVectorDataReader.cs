using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Reader;

internal sealed class GuidVectorDataReader : VectorDataReaderBase
{
    internal unsafe GuidVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

#if NET8_0_OR_GREATER
    protected override T GetValidValue<T>(ulong offset, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType)
#else
    protected override T GetValidValue<T>(ulong offset, Type targetType)
#endif
    {
        if (DuckDBType != DuckDBType.Uuid)
        {
            return base.GetValidValue<T>(offset, targetType);
        }

        var hugeInt = GetFieldData<DuckDBHugeInt>(offset);

        var guid = hugeInt.ConvertToGuid();
        return (T)(object)guid;
    }

#if NET8_0_OR_GREATER
    internal override object GetValue(ulong offset, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType)
#else
    internal override object GetValue(ulong offset, Type targetType)
#endif
    {
        if (DuckDBType != DuckDBType.Uuid)
        {
            return base.GetValue(offset, targetType);
        }

        var hugeInt = GetFieldData<DuckDBHugeInt>(offset);

        return hugeInt.ConvertToGuid();
    }
}