using System;
using System.IO;
using System.Text;

namespace DuckDB.NET.Data.Internal.Reader;

internal class StringVectorDataReader : VectorDataReaderBase
{
    internal unsafe StringVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

    internal override T GetValue<T>(ulong offset)
    {
        return DuckDBType switch
        {
            DuckDBType.Blob => (T)(object)GetStream(offset),
            DuckDBType.Varchar => (T)(object)GetString(offset),
            _ => base.GetValue<T>(offset)
        };
    }

    internal override object GetValue(ulong offset, Type? targetType = null)
    {
        return DuckDBType switch
        {
            DuckDBType.Blob => GetStream(offset),
            DuckDBType.Varchar => GetString(offset),
            _ => base.GetValue(offset, targetType)
        };
    }

    private unsafe string GetString(ulong offset)
    {
        var data = (DuckDBString*)DataPointer + offset;

        return new string(data->Data, 0, data->Length, Encoding.UTF8);
    }

    private unsafe Stream GetStream(ulong offset)
    {
        var data = (DuckDBString*)DataPointer + offset;

        return new UnmanagedMemoryStream((byte*)data->Data, data->Length, data->Length, FileAccess.Read);
    }
}