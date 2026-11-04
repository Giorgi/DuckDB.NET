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

    // An Int128 has the bytes of a HUGEINT and a UInt128 those of a UHUGEINT, on a little-endian machine, so they are
    // stored as they are, in either of the two columns, like the other integers: an Int128 of -1 in a UHUGEINT column is
    // 2^128 - 1, as a long of -1 in a UBIGINT column is 2^64 - 1. Any other column refuses them: the 16 bytes would
    // overwrite the next value of a narrower column, or run past the vector.
    internal override bool AppendNumeric(Int128 value, ulong rowIndex) => ColumnType is DuckDBType.HugeInt or DuckDBType.UnsignedHugeInt
        ? AppendValueInternal(value, rowIndex)
        : base.AppendNumeric(value, rowIndex);

    internal override bool AppendNumeric(UInt128 value, ulong rowIndex) => ColumnType is DuckDBType.HugeInt or DuckDBType.UnsignedHugeInt
        ? AppendValueInternal(value, rowIndex)
        : base.AppendNumeric(value, rowIndex);
}
