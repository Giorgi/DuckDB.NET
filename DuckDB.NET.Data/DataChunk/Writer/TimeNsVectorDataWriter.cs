namespace DuckDB.NET.Data.DataChunk.Writer;

internal sealed unsafe class TimeNsVectorDataWriter(IntPtr vector, void* vectorData) : VectorDataWriterBase(vector, vectorData, DuckDBType.TimeNs)
{
    internal override bool AppendDateTime(DateTime value, ulong rowIndex) =>
        AppendValueInternal(value.ToDuckDBTimeNs(), rowIndex);

    internal override bool AppendTimeOnly(TimeOnly value, ulong rowIndex) =>
        AppendValueInternal(value.ToDuckDBTimeNs(), rowIndex);

    internal override bool AppendTimeOnly(DuckDBTimeOnly value, ulong rowIndex) =>
        AppendValueInternal(value.ToDuckDBTimeNs(), rowIndex);
}
