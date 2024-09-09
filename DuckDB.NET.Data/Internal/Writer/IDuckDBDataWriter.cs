using System.Diagnostics.CodeAnalysis;

namespace DuckDB.NET.Data.Writer;

#if NET8_0_OR_GREATER
[Experimental("DuckDBNET001")]
public interface IDuckDBDataWriter
{
    unsafe void WriteNull(ulong rowIndex);
    unsafe void WriteValue<T>(T value, ulong rowIndex);
}
#endif