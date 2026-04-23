namespace DuckDB.NET.Data.DataChunk.Writer;

public interface IDuckDBDataWriter
{
    void WriteNull(ulong rowIndex);
    void WriteValue<T>(T value, ulong rowIndex);
}
