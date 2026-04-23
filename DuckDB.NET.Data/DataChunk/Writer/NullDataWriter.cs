namespace DuckDB.NET.Data.DataChunk.Writer;

internal sealed class NullDataWriter : IDuckDBDataWriter
{
    public static readonly NullDataWriter Instance = new();

    private NullDataWriter() { }

    public void WriteNull(ulong rowIndex) { }

    public void WriteValue<T>(T value, ulong rowIndex) { }
    
    public void Dispose() { }
}
