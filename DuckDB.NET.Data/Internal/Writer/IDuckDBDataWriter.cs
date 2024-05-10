namespace DuckDB.NET.Data.Writer;

public interface IDuckDBDataWriter
{
    unsafe void AppendNull(int rowIndex);
    unsafe void AppendValue<T>(T value, int rowIndex);
}