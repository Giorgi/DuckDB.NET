using System;

namespace DuckDB.NET.Data.Internal.Reader;

internal static class VectorDataReaderFactory
{
    public static unsafe VectorDataReaderBase CreateReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType)
    {
        return columnType switch
        {
            DuckDBType.Date => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType),
            DuckDBType.Time => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType),
            DuckDBType.Interval => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType),
            DuckDBType.Timestamp => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType),
            
            DuckDBType.Boolean => new BooleanVectorDataReader(dataPointer, validityMaskPointer, columnType),

            DuckDBType.List => new ListVectorDataReader(vector, dataPointer, validityMaskPointer, columnType),
            DuckDBType.Blob => new StringVectorDataReader(dataPointer, validityMaskPointer, columnType),
            DuckDBType.Varchar => new StringVectorDataReader(dataPointer, validityMaskPointer, columnType),
            DuckDBType.Enum => new EnumVectorDataReader(vector, dataPointer, validityMaskPointer, columnType),
            DuckDBType.Struct => new StructVectorDataReader(vector, dataPointer, validityMaskPointer, columnType),
            DuckDBType.Decimal => new DecimalVectorDataReader(vector, dataPointer, validityMaskPointer, columnType),
            _ => new NumericVectorDataReader(dataPointer, validityMaskPointer, columnType)
        };
    }
}