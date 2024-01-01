using System;
using System.Collections;
using System.Collections.Generic;
using System.Numerics;
using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Data.Internal.Reader;

internal class MapVectorDataReader : VectorDataReaderBase
{
    private readonly VectorDataReaderBase keyReader;
    private readonly VectorDataReaderBase valueReader;

    internal unsafe MapVectorDataReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        using var logicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(vector);
        using var keyTypeLogical = NativeMethods.LogicalType.DuckDBMapTypeKeyType(logicalType);
        using var valueTypeLogical = NativeMethods.LogicalType.DuckDBMapTypeValueType(logicalType);

        var keyType = NativeMethods.LogicalType.DuckDBGetTypeId(keyTypeLogical);
        var valueType = NativeMethods.LogicalType.DuckDBGetTypeId(valueTypeLogical);

        var childVector = NativeMethods.DataChunks.DuckDBListVectorGetChild(vector);

        var keyVector = NativeMethods.DataChunks.DuckDBStructVectorGetChild(childVector, 0);
        var valueVector = NativeMethods.DataChunks.DuckDBStructVectorGetChild(childVector, 1);

        keyReader = VectorDataReaderFactory.CreateReader(keyVector, NativeMethods.DataChunks.DuckDBVectorGetData(keyVector),
                                                                    NativeMethods.DataChunks.DuckDBVectorGetValidity(keyVector), keyType, columnName);

        valueReader = VectorDataReaderFactory.CreateReader(valueVector, NativeMethods.DataChunks.DuckDBVectorGetData(valueVector),
                                                                        NativeMethods.DataChunks.DuckDBVectorGetValidity(valueVector), valueType, columnName);
    }

    protected override Type GetColumnType()
    {
        return typeof(Dictionary<,>).MakeGenericType(keyReader.ClrType, valueReader.ClrType);
    }

    protected override Type GetColumnProviderSpecificType()
    {
        return typeof(Dictionary<,>).MakeGenericType(keyReader.ProviderSpecificClrType, valueReader.ProviderSpecificClrType);
    }

    internal override unsafe object GetValue(ulong offset, Type targetType)
    {
        var allowsNullValues = true;
        var arguments = targetType.GetGenericArguments();

        allowsNullValues = arguments.Length == 2 && Nullable.GetUnderlyingType(arguments[1]) != null;

        if (Activator.CreateInstance(targetType) is IDictionary instance)
        {
            var listData = (DuckDBListEntry*)DataPointer + offset;

            for (ulong i = 0; i < listData->Length; i++)
            {
                var childOffset = i + listData->Offset;

                var key = keyReader.GetValue(childOffset);
                var value = valueReader.IsValid(childOffset) ? valueReader.GetValue(childOffset) : null;

                if (allowsNullValues || value != null)
                {
                    instance.Add(key, value);
                }
                else
                {
                    throw new NullReferenceException($"The Map in column {ColumnName} contains null value but dictionary does not allow null values");
                }
            }

            return instance;
        }
        else
        {
            throw new InvalidOperationException($"Cannot read Map column {ColumnName} in a non-dictionary type");
        }
    }
}