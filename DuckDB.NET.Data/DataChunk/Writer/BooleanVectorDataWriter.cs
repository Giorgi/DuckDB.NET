namespace DuckDB.NET.Data.DataChunk.Writer;

internal sealed unsafe class BooleanVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    internal override bool AppendBool(bool value, ulong rowIndex) => AppendValueInternal(value, rowIndex);
}
