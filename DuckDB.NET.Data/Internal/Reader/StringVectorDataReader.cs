using System;
using System.IO;
using System.Text;

namespace DuckDB.NET.Data.Internal.Reader;

internal class StringVectorDataReader : VectorDataReaderBase
{
    private const int InlineStringMaxLength = 12;

    internal unsafe StringVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType) : base(dataPointer, validityMaskPointer, columnType)
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
        var length = *(int*)data;

        var pointer = length <= InlineStringMaxLength
            ? data->value.inlined.inlined
            : data->value.pointer.ptr;

        return new string(pointer, 0, length, Encoding.UTF8);
    }

    private unsafe Stream GetStream(ulong offset)
    {
        var data = (DuckDBString*)DataPointer + offset;
        var length = *(int*)data;

        if (length <= InlineStringMaxLength)
        {
            var value = new string(data->value.inlined.inlined, 0, length, Encoding.UTF8);
            return new MemoryStream(Encoding.UTF8.GetBytes(value), false);
        }

        return new UnmanagedMemoryStream((byte*)data->value.pointer.ptr, length, length, FileAccess.Read);
    }
}