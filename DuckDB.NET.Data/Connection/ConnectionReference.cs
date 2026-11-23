namespace DuckDB.NET.Data.Connection;

/// <summary>
/// Just makes it easier to pass/receive this data from the ConnectionManager
/// </summary>
internal class ConnectionReference(FileReference fileReferenceCounter, DuckDBV2Connection nativeConnection)
{
    public FileReference FileReferenceCounter { get; } = fileReferenceCounter;
    public DuckDBV2Connection NativeConnection { get; } = nativeConnection;

    public override string? ToString() => FileReferenceCounter?.ToString();
}