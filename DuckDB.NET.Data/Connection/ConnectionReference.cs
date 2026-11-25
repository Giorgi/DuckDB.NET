namespace DuckDB.NET.Data.Connection;

/// <summary>
/// The native objects of an open <see cref="DuckDBConnection"/>: the instance handle it opened and its connection.
/// </summary>
internal class ConnectionReference(string dataSource, DuckDBV2Instance instance, DuckDBV2Connection nativeConnection)
{
    public string DataSource { get; } = dataSource;
    public DuckDBV2Instance Instance { get; } = instance;
    public DuckDBV2Connection NativeConnection { get; } = nativeConnection;

    public override string ToString() => DataSource;
}
