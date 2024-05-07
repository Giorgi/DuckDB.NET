﻿using System;
using System.Diagnostics;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Reader;

internal class GuidVectorDataReader : VectorDataReaderBase
{
    internal unsafe GuidVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

    protected override T GetValidValue<T>(ulong offset, Type targetType)
    {
        if (DuckDBType != DuckDBType.Uuid)
        {
            return base.GetValidValue<T>(offset, targetType);
        }

        var hugeInt = GetFieldData<DuckDBHugeInt>(offset);

        var guid = hugeInt.ConvertToGuid();
        return (T)(object)guid;
    }

    internal override unsafe object GetValue(ulong offset, Type targetType)
    {
        if (DuckDBType != DuckDBType.Uuid)
        {
            return base.GetValue(offset, targetType);
        }

        var hugeInt = GetFieldData<DuckDBHugeInt>(offset);

        return hugeInt.ConvertToGuid();
    }
}