using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace DuckDB.NET.Data.Internal
{
    /// <summary>
    /// Creates, caches, and disconnects ConnectionReferences.
    /// </summary>
    internal class ConnectionManager
    {
        public static readonly ConnectionManager Default = new ConnectionManager();

        private static readonly ConcurrentDictionary<string, FileRef> ConnectionCache = new(StringComparer.OrdinalIgnoreCase);

        internal ConnectionReference GetConnectionReference(string connectionString)
        {
            string filename = GetFileName(connectionString);

            FileRef fileRef = null;

            //need to loop until we have a locked fileRef
            //that is also in the cache
            do
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
            while (fileRef == null);

            //now connect what needs to be connected
            try
            {
                if (fileRef.Database == null)
                {
                    var inMemory = filename == string.Empty;
                    var filenameForDll = inMemory ? null : filename;

                    var resultOpen = PlatformIndependentBindings.NativeMethods.DuckDBOpen(filenameForDll, out fileRef.Database);

                    if (!resultOpen.IsSuccess())
                    {
                        throw new DuckDBException("DuckDBOpen failed", resultOpen);
                    }
                }

                var resultConnect = PlatformIndependentBindings.NativeMethods.DuckDBConnect(fileRef.Database, out DuckDBNativeConnection nativeConnection);

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
                Monitor.Exit(fileRef);
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
                    fileRef.Database.Dispose();
                    fileRef.Database = null;

                    if (!ConnectionCache.TryRemove(fileRef.FileName, out _))
                    {
                        throw new InvalidOperationException($"Internal Error: tried to remove {fileRef.FileName} from cache but it wasn't there!");
                    }
                }
            }
        }
                
        private string GetFileName(string connectionString)
        {
            string filename = null;

            if (connectionString.StartsWith("Data Source=") || connectionString.StartsWith("DataSource="))
            {
                var strings = connectionString.Split('=');

                if (strings[1] == ":memory:")
                {
                    filename = "";
                }
                else
                {
                    filename = strings[1];
                }
            }

            if (filename == null)
            {
                throw new InvalidOperationException($"ConnectionString '{connectionString}' is not valid");
            }

            return filename;
        }
    }
}