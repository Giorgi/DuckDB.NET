
namespace DuckDB.NET.Data.Connection;

/// <summary>
/// Opens and closes the native connections of <see cref="DuckDBConnection"/>s.
/// </summary>
/// <remarks>
/// DuckDB shares the instances itself (<c>instance_open_shared</c>, see <see cref="DuckDBV2Startup.OpenInstance"/>):
/// one per database file, as DuckDB resolves its path, and one per named in-memory database. Waiting for an instance
/// that is closing before a file is opened again, which keeps two instances off one file, is DuckDB's too. Each open
/// gets a handle of its own; <see cref="DuckDBConnection.Duplicate"/> shares the handle of the connection it copies,
/// which is how a duplicate reaches a private in-memory database, so a handle is counted and destroyed with its last
/// connection.
/// </remarks>
internal class ConnectionManager
{
    public static readonly ConnectionManager Default = new();

    internal ConnectionReference GetConnectionReference(DuckDBConnectionString connectionString)
    {
        var fileRef = new FileReference(connectionString.DataSource)
        {
            // The configuration goes in as the instance's startup options, all in one call: a rejected option
            // fails the open with DuckDB's message, which names the option.
            Database = DuckDBV2Startup.OpenInstance(connectionString.InstancePath, connectionString.Configuration)
        };

        try
        {
            var nativeConnection = DuckDBV2Startup.Connect(fileRef.Database);
            fileRef.Increment();
            return new ConnectionReference(fileRef, nativeConnection);
        }
        catch
        {
            fileRef.Database.Dispose();
            fileRef.Database = null;
            throw;
        }
    }

    internal void ReturnConnectionReference(ConnectionReference connectionReference)
    {
        var fileRef = connectionReference.FileReferenceCounter;

        lock (fileRef)
        {
            var nativeConnection = connectionReference.NativeConnection;
            nativeConnection.Dispose();

            var current = fileRef.Decrement();

            if (current < 0)
            {
                throw new InvalidOperationException($"{fileRef.FileName} has been returned too many times");
            }

            if (current == 0)
            {
                // DuckDB closes the instance once no handle and no connection is left.
                fileRef.Database?.Dispose();
                fileRef.Database = null;
            }
        }
    }

    internal ConnectionReference DuplicateConnectionReference(ConnectionReference connectionReference)
    {
        var fileRef = connectionReference.FileReferenceCounter;
        if (fileRef.Database is null)
            throw new InvalidOperationException(); //shouldn't happen if we already have a connection reference

        lock (fileRef)
        {
            var duplicatedNativeConnection = DuckDBV2Startup.Connect(fileRef.Database);
            fileRef.Increment();
            return new ConnectionReference(fileRef, duplicatedNativeConnection);
        }
    }
}
