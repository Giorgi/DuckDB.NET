using System;

namespace DuckDB.NET.Data.Internal.Reader;

static class VectorDataReaderFactory
{
    public static unsafe VectorDataReader CreateReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType)
    {
        return columnType switch
        {
            DuckDBType.List => new ListVectorDataReader(vector, dataPointer, validityMaskPointer, columnType),
            DuckDBType.Struct => new StructVectorDataReader(vector, dataPointer, validityMaskPointer, columnType),
            _ => new VectorDataReader(vector, dataPointer, validityMaskPointer, columnType)
        };
    }
}