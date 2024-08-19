using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer;

internal static class VectorDataWriterFactory
{
    public static unsafe VectorDataWriterBase CreateWriter(IntPtr vector, DuckDBLogicalType logicalType)
    {
        var dataPointer = NativeMethods.Vectors.DuckDBVectorGetData(vector);
        var columnType = NativeMethods.LogicalType.DuckDBGetTypeId(logicalType);

        return columnType switch
        {
            DuckDBType.Uuid => new GuidVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.Date => new DateTimeVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.Time => new DateTimeVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.TimeTz => new DateTimeVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.Interval => new IntervalVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.Timestamp => new DateTimeVectorDataWriter(vector, dataPointer, columnType),
                
            DuckDBType.Boolean => new BooleanVectorDataWriter(vector, dataPointer, columnType),

            DuckDBType.Map => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            DuckDBType.List => new ListVectorDataWriter(vector, dataPointer, columnType, logicalType),
            DuckDBType.Array => new ListVectorDataWriter(vector, dataPointer, columnType, logicalType),
            DuckDBType.Blob => new StringVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.Varchar => new StringVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.Bit => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            DuckDBType.Enum => new EnumVectorDataWriter(vector, dataPointer, logicalType, columnType),
            DuckDBType.Struct => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            DuckDBType.Decimal => new DecimalVectorDataWriter(vector, dataPointer, logicalType, columnType),
            DuckDBType.TimestampS => new DateTimeVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.TimestampMs => new DateTimeVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.TimestampNs => new DateTimeVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.TimestampTz => new DateTimeVectorDataWriter(vector, dataPointer, columnType),
            _ => new NumericVectorDataWriter(vector, dataPointer, columnType)
        };
    }
}