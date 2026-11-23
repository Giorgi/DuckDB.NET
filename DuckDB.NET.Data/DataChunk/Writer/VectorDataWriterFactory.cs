using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.Execution;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.DataChunk.Writer;

internal static class VectorDataWriterFactory
{
    /// <summary>A writer for a V2 vector of the given type: a data chunk's column, or a LIST's or ARRAY's items.</summary>
    public static unsafe VectorDataWriterBase CreateWriter(_duckdb_v2_vector* v2Vector, _duckdb_v2_logical_type* logicalType)
    {
        var vector = (IntPtr)v2Vector;
        var dataPointer = DuckDBV2Vectors.GetMutableData(v2Vector);
        var columnType = DuckDBV2LogicalTypes.GetId(logicalType).ToDuckDBType();

        return columnType switch
        {
            DuckDBType.Uuid => new GuidVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.Date => new DateTimeVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.Time => new TimeVectorDataWriter<DuckDBTime>(vector, dataPointer, columnType),
            DuckDBType.TimeNs => new TimeVectorDataWriter<DuckDBTimeNs>(vector, dataPointer, columnType),
            DuckDBType.TimeTz => new DateTimeVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.Interval => new IntervalVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.Timestamp => new DateTimeVectorDataWriter(vector, dataPointer, columnType),
                
            DuckDBType.Boolean => new BooleanVectorDataWriter(vector, dataPointer, columnType),

            DuckDBType.Map => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            DuckDBType.List => new ListVectorDataWriter(vector, dataPointer, columnType, logicalType),
            DuckDBType.Array => new ArrayVectorDataWriter(vector, dataPointer, columnType, logicalType),
            DuckDBType.Blob => new StringVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.Varchar => new StringVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.Bit => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            DuckDBType.Enum => new EnumVectorDataWriter(vector, dataPointer, logicalType, columnType),
            DuckDBType.Struct => throw new NotImplementedException($"Writing {columnType} to data chunk is not yet supported"),
            DuckDBType.Decimal => new DecimalVectorDataWriter(vector, dataPointer, (byte)DuckDBV2LogicalTypes.GetUIntParam(logicalType, 0), (byte)DuckDBV2LogicalTypes.GetUIntParam(logicalType, 1), columnType),
            DuckDBType.TimestampS => new DateTimeVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.TimestampMs => new DateTimeVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.TimestampNs => new DateTimeVectorDataWriter(vector, dataPointer, columnType),
            DuckDBType.TimestampTz => new DateTimeVectorDataWriter(vector, dataPointer, columnType),
            _ => new NumericVectorDataWriter(vector, dataPointer, columnType)
        };
    }
}
