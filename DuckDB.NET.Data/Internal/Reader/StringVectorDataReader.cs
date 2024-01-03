using System;
using System.Collections;
using System.IO;
using System.Text;

namespace DuckDB.NET.Data.Internal.Reader;

internal class StringVectorDataReader : VectorDataReaderBase
{
    internal unsafe StringVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

    protected override T GetValidValue<T>(ulong offset, Type targetType)
    {
        return DuckDBType switch
        {
            DuckDBType.Bit => GetBitString<T>(offset),
            DuckDBType.Blob => (T)(object)GetStream(offset),
            DuckDBType.Varchar => (T)(object)GetString(offset),
            _ => base.GetValidValue<T>(offset, targetType)
        };
    }

    internal override object GetValue(ulong offset, Type targetType)
    {
        return DuckDBType switch
        {
            DuckDBType.Bit => GetBitString(offset),
            DuckDBType.Blob => GetStream(offset),
            DuckDBType.Varchar => GetString(offset),
            _ => base.GetValue(offset, targetType)
        };
    }

    private T GetBitString<T>(ulong offset)
    {
        if (typeof(T) == typeof(string))
        {
            return (T)(object)GetBitString(offset);
        }

        if (typeof(T) == typeof(BitArray))
        {
            return (T)(object)GetBitStringAsBitArray(offset);
        }

        return base.GetValue<T>(offset);
    }

    private string GetBitString(ulong offset)
    {
        var bitArray = GetBitStringAsBitArray(offset);

        var output = new char[bitArray.Length];

        for (var index = 0; index < bitArray.Count; index++)
        {
            output[index] = bitArray[index] ? '1' : '0';
        }

        return new string(output);
    }

    //Copied from https://github.com/duckdb/duckdb/blob/8a17511028d306561d88da9425f9e0e88dedd70c/src/common/types/bit.cpp#L63
    private unsafe BitArray GetBitStringAsBitArray(ulong offset)
    {
        var bits = (DuckDBString*)DataPointer + offset;
        var data = bits->Data;

        var bitLength = (bits->Length - 1) * 8 - *data;

        var output = new BitArray(bitLength);
        var outputIndex = 0;

        for (var bitIndex = *data; bitIndex < 8; bitIndex++)
        {
            output[outputIndex++] = (*(data + 1) & (1 << (7 - bitIndex))) > 0;
        }

        for (var byteIndex = 2; byteIndex < bits->Length; byteIndex++)
        {
            for (var bitIndex = 0; bitIndex < 8; bitIndex++)
            {
                output[outputIndex++] = (*(data + byteIndex) & (1 << (7 - bitIndex))) > 0;
            }
        }

        return output;
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