namespace DuckDB.NET.Data.DataChunk.Writer;

internal sealed unsafe class NumericVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    internal override bool AppendNumeric<T>(T value, ulong rowIndex) => AppendValueInternal(value, rowIndex);

    internal override bool AppendBigInteger(BigInteger value, ulong rowIndex) => ColumnType == DuckDBType.UnsignedHugeInt
        ? AppendValueInternal(new DuckDBUHugeInt(value), rowIndex)
        : AppendValueInternal(new DuckDBHugeInt(value), rowIndex);
}
