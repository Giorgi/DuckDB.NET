namespace DuckDB.NET.Data.DataChunk.Writer;

// A DATE column: 4 bytes, the days from 1970-01-01. A DateOnly, a DuckDBDateOnly and a DateTime (its date) are its only
// types. TIMETZ and the timestamps, which are 8 bytes, have writers of their own, so a date is refused there: it used
// to be written as these 4 bytes, two rows to a slot of 8, and the second half of every chunk was left as it was.
internal sealed unsafe class DateVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    internal override bool AppendDateOnly(DateOnly value, ulong rowIndex) => AppendValueInternal(value.ToDuckDBDate(), rowIndex);

    // A DuckDBDateOnly holds a year, a month and a day, and DuckDB puts them together.
    internal override bool AppendDateOnly(DuckDBDateOnly value, ulong rowIndex) => AppendValueInternal(value.ToDuckDBDate(), rowIndex);

    internal override bool AppendDateTime(DateTime value, ulong rowIndex) => AppendValueInternal(value.ToDuckDBDate(), rowIndex);
}
