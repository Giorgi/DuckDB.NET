using System.Collections.Concurrent;
using System.Threading;

namespace DuckDB.NET.Data.Connection;

/// <summary>
/// Creates, caches, and disconnects ConnectionReferences.
/// </summary>
internal class ConnectionManager
{
    public static readonly ConnectionManager Default = new();

    private static readonly ConcurrentDictionary<string, FileReference> ConnectionCache = new(StringComparer.OrdinalIgnoreCase);

    internal ConnectionReference GetConnectionReference(DuckDBConnectionString connectionString)
    {
        var filename = connectionString.DataSource;

        var fileRef = connectionString.InMemory && !connectionString.Shared ? new FileReference("") : null;

        //need to loop until we have a locked fileRef
        //that is also in the cache
        while (fileRef == null)
        {
            fileRef = ConnectionCache.GetOrAdd(filename, fn =>
            {
                fileRef = new FileReference(filename);
                return fileRef;
            });

            Monitor.Enter(fileRef);

            //Need to make sure what we have locked is still in the cache
            var existingFileRef = ConnectionCache.GetOrAdd(filename, fileRef);

            if (existingFileRef == fileRef)
            {
                //file in the cache matches what is locked, we are good!
                break;
            }

            //exit lock and try the whole thing again
            Monitor.Exit(fileRef);
            fileRef = null;
        }

        //now connect what needs to be connected
        try
        {
            if (fileRef.Database == null)
            {
                var path = connectionString.InMemory ? null : filename;

                // The configuration goes in as the instance's startup options, all in one call: a rejected option
                // fails the open with DuckDB's message, which names the option.
                fileRef.Database = DuckDBV2Startup.OpenInstance(path, connectionString.Configuration);
            }

            var nativeConnection = DuckDBV2Startup.Connect(fileRef.Database);
            fileRef.Increment();

            return new ConnectionReference(fileRef, nativeConnection);
        }
        finally
        {
            if (Monitor.IsEntered(fileRef))
            {
                Monitor.Exit(fileRef);
            }
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
                fileRef.Database?.Dispose();
                fileRef.Database = null;

                if (!string.IsNullOrEmpty(fileRef.FileName) && !ConnectionCache.TryRemove(fileRef.FileName, out _))
                {
                    throw new InvalidOperationException($"Internal Error: tried to remove {fileRef.FileName} from cache but it wasn't there!");
                }
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