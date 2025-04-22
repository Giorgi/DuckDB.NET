using System.Diagnostics.CodeAnalysis;

#if NET8_0_OR_GREATER
namespace DuckDB.NET.Data.DataChunk.Writer;

[Experimental("DuckDBNET001")]
public interface IDuckDBDataWriter
{
    void WriteNull(ulong rowIndex);
    void WriteValue<T>(T value, ulong rowIndex);
}
#endif