using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal;

/// <summary>
/// Just makes it easier to pass/receive this data from the ConnectionManager
/// </summary>
internal class ConnectionReference(FileRef fileRefCounter, DuckDBNativeConnection nativeConnection)
{
    public FileRef FileRefCounter { get; private set; } = fileRefCounter;
    public DuckDBNativeConnection NativeConnection { get; private set; } = nativeConnection;

    public override string? ToString() => FileRefCounter?.ToString();
}