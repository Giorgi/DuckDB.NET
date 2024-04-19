using System;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal.Writer
{
    internal static class VectorDataWriterFactory
    {
        public static unsafe VectorDataWriterBase CreateWriter(IntPtr vector, void* dataPointer, DuckDBLogicalType logicalType)
        {
            var columnType = NativeMethods.LogicalType.DuckDBGetTypeId(logicalType);
            return columnType switch
            {
                DuckDBType.Uuid => new GuidVectorDataWriter(vector, dataPointer),
                DuckDBType.Interval => new IntervalVectorDataWriter(vector, dataPointer),
                DuckDBType.Decimal => new DecimalVectorDataWriter(vector, dataPointer, logicalType),
                _ => new VectorDataWriterBase(vector, dataPointer)
            };
        }
    }
}
