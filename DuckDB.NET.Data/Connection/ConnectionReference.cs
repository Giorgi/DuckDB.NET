namespace DuckDB.NET.Data.Connection;

/// <summary>
/// Just makes it easier to pass/receive this data from the ConnectionManager
/// </summary>
internal class ConnectionReference(FileReference fileReferenceCounter, DuckDBNativeConnection nativeConnection)
{
    public FileReference FileReferenceCounter { get; } = fileReferenceCounter;
    public DuckDBNativeConnection NativeConnection { get; private set; } = nativeConnection;

    public override string? ToString() => FileReferenceCounter?.ToString();
}