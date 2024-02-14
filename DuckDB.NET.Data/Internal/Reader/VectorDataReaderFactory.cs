using System;

namespace DuckDB.NET.Data.Internal.Reader;

internal static class VectorDataReaderFactory
{
    public static unsafe VectorDataReaderBase CreateReader(IntPtr vector, void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName)
    {
        return columnType switch
        {
            DuckDBType.Uuid => new GuidVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Date => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Time => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Interval => new IntervalVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Timestamp => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),

            DuckDBType.Boolean => new BooleanVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),

            DuckDBType.Map => new MapVectorDataReader(vector, dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.List => new ListVectorDataReader(vector, dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Blob => new StringVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Varchar => new StringVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Bit => new StringVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Enum => new EnumVectorDataReader(vector, dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Struct => new StructVectorDataReader(vector, dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.Decimal => new DecimalVectorDataReader(vector, dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.TimestampS => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.TimestampMs => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.TimestampNs => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            DuckDBType.TimestampTz => new DateTimeVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName),
            _ => new NumericVectorDataReader(dataPointer, validityMaskPointer, columnType, columnName)
        };
    }
}