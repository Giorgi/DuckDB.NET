using System;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using DuckDB.NET.Data.ConnectionString;

namespace DuckDB.NET.Data.Internal;

/// <summary>
/// Creates, caches, and disconnects ConnectionReferences.
/// </summary>
internal class ConnectionManager
{
    public static readonly ConnectionManager Default = new();

    private static readonly ConcurrentDictionary<string, FileRef> ConnectionCache = new(StringComparer.OrdinalIgnoreCase);

    internal ConnectionReference GetConnectionReference(DuckDBConnectionString connectionString)
    {
        var filename = connectionString.DataSource;

        var fileRef = connectionString.InMemory && !connectionString.Shared ? new FileRef("") : null;

        //need to loop until we have a locked fileRef
        //that is also in the cache
        while (fileRef == null)
        {
            fileRef = ConnectionCache.GetOrAdd(filename, fn =>
            {
                fileRef = new FileRef(filename);
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

                NativeMethods.Configure.DuckDBCreateConfig(out var config);
                using (config)
                {
                    foreach (var (option, value) in connectionString.Configuration)
                    {
                        var state = NativeMethods.Configure.DuckDBSetConfig(config, option, value);

                        if (!state.IsSuccess())
                        {
                            throw new DuckDBException($"Error setting '{option}' to '{value}'");
                        }
                    }

                    var resultOpen = NativeMethods.Startup.DuckDBOpen(path, out var db, config, out var error);

                    if (!resultOpen.IsSuccess())
                    {
                        throw new DuckDBException($"DuckDBOpen failed: {error.ToManagedString()}", resultOpen);
                    }
                    fileRef.Database = db; 
                }
            }

            var resultConnect = NativeMethods.Startup.DuckDBConnect(fileRef.Database, out var nativeConnection);

            if (resultConnect.IsSuccess())
            {
                fileRef.Increment();
            }
            else
            {
                throw new DuckDBException("DuckDBConnect failed", resultConnect);
            }

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
        var fileRef = connectionReference.FileRefCounter;

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
        var fileRef = connectionReference.FileRefCounter;

        if (fileRef.Database is null)
            throw new InvalidOperationException(); //shouldn't happen if we already have a connection reference

        lock (fileRef)
        {
            var resultConnect = NativeMethods.Startup.DuckDBConnect(fileRef.Database, out var duplicatedNativeConnection);
            if (resultConnect.IsSuccess())
            {
                fileRef.Increment();
            }
            else
            {
                throw new DuckDBException("DuckDBConnect failed", resultConnect);
            }
            return new ConnectionReference(fileRef, duplicatedNativeConnection);
        }
    }
}