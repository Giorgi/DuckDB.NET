using System;
using DuckDB.NET.Data.Internal.Reader;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer
{
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
                DuckDBType.Interval => new IntervalVectorDataWriter(vector, dataPointer, columnType),
                DuckDBType.Timestamp => new DateTimeVectorDataWriter(vector, dataPointer, columnType),
                
                DuckDBType.Boolean => new BooleanVectorDataWriter(vector, dataPointer, columnType),

                DuckDBType.Blob => new StringVectorDataWriter(vector, dataPointer, columnType),
                DuckDBType.Varchar => new StringVectorDataWriter(vector, dataPointer, columnType),

                DuckDBType.Decimal => new DecimalVectorDataWriter(vector, dataPointer, logicalType, columnType),
                _ => new NumericVectorDataWriter(vector, dataPointer, columnType)
            };
        }
    }
}
