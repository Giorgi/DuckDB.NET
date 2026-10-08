namespace DuckDB.NET.Data.DataChunk.Writer;

internal sealed unsafe class NumericVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    internal override bool AppendNumeric(sbyte value, ulong rowIndex) => AppendValueInternal(value, rowIndex);

    internal override bool AppendNumeric(short value, ulong rowIndex) => AppendValueInternal(value, rowIndex);

    internal override bool AppendNumeric(int value, ulong rowIndex) => AppendValueInternal(value, rowIndex);

    internal override bool AppendNumeric(long value, ulong rowIndex) => AppendValueInternal(value, rowIndex);

    internal override bool AppendNumeric(byte value, ulong rowIndex) => AppendValueInternal(value, rowIndex);

    internal override bool AppendNumeric(ushort value, ulong rowIndex) => AppendValueInternal(value, rowIndex);

    internal override bool AppendNumeric(uint value, ulong rowIndex) => AppendValueInternal(value, rowIndex);

    internal override bool AppendNumeric(ulong value, ulong rowIndex) => AppendValueInternal(value, rowIndex);

    internal override bool AppendNumeric(float value, ulong rowIndex) => AppendValueInternal(value, rowIndex);

    internal override bool AppendNumeric(double value, ulong rowIndex) => AppendValueInternal(value, rowIndex);

    internal override bool AppendBigInteger(BigInteger value, ulong rowIndex) => ColumnType == DuckDBType.UnsignedHugeInt
        ? AppendValueInternal(new DuckDBUHugeInt(value), rowIndex)
        : AppendValueInternal(new DuckDBHugeInt(value), rowIndex);
}
