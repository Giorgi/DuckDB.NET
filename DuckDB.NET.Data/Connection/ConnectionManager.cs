namespace DuckDB.NET.Data.Connection;

/// <summary>
/// Opens and closes the native connections of <see cref="DuckDBConnection"/>s.
/// </summary>
/// <remarks>
/// DuckDB shares the instances itself (<c>instance_open_shared</c>, see <see cref="DuckDBV2Startup.OpenInstance"/>):
/// one per database file, as DuckDB resolves its path, and one per named in-memory database. Waiting for an instance
/// that is closing before a file is opened again, which keeps two instances off one file, is DuckDB's too. Every open
/// gets an instance handle of its own, which is destroyed with its connection; DuckDB closes the instance once no
/// handle and no connection is left.
/// </remarks>
internal class ConnectionManager
{
    public static readonly ConnectionManager Default = new();

    internal ConnectionReference GetConnectionReference(DuckDBConnectionString connectionString)
    {
        // The configuration goes in as the instance's startup options, all in one call: a rejected option fails the
        // open with DuckDB's message, which names the option.
        var instance = DuckDBV2Startup.OpenInstance(connectionString.InstancePath, connectionString.Configuration);

        try
        {
            return new ConnectionReference(connectionString.DataSource, instance, DuckDBV2Startup.Connect(instance));
        }
        catch
        {
            instance.Dispose();
            throw;
        }
    }

    internal void ReturnConnectionReference(ConnectionReference connectionReference)
    {
        connectionReference.NativeConnection.Dispose();
        connectionReference.Instance.Dispose();
    }
}
