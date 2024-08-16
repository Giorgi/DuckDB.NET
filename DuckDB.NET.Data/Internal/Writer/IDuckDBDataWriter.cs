namespace DuckDB.NET.Data.Writer;

public interface IDuckDBDataWriter
{
    unsafe void AppendNull(ulong rowIndex);
    unsafe void AppendValue<T>(T value, ulong rowIndex);
}