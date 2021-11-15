using System;
using System.Collections.Generic;
using System.Threading;

namespace DuckDB.NET.Data.Internal
{
    /// <summary>
    /// Creates, caches, and disconnects ConnectionReferences
    /// </summary>
    internal class ConnectionManager
    {
        public static readonly ConnectionManager Default = new ConnectionManager();
        private static Dictionary<string, FileRefCounter> connections = new(StringComparer.OrdinalIgnoreCase);

        internal ConnectionReference GetConnectionReference(string connectionString)
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

            lock (connections)
            {
                if (!connections.TryGetValue(filename, out FileRefCounter dbFile))
                {
                    dbFile = new FileRefCounter(filename);
                    connections.Add(filename, dbFile);
                }

                if (dbFile.Database == null)
                {
                    var inMemory = filename == string.Empty;
                    var filenameForDll = inMemory ? null : filename;

                    var resultOpen = PlatformIndependentBindings.NativeMethods.DuckDBOpen(filenameForDll, out dbFile.Database);

                    if (!resultOpen.IsSuccess())
                    {
                        throw new DuckDBException("DuckDBOpen failed", resultOpen);
                    }
                }

                var resultConnect = PlatformIndependentBindings.NativeMethods.DuckDBConnect(dbFile.Database, out DuckDBNativeConnection nativeConnection);

                if (resultConnect.IsSuccess())
                {
                    Interlocked.Increment(ref dbFile.ConnectionCount);
                }
                else
                {
                    throw new DuckDBException("DuckDBConnect failed", resultConnect);
                }

                return new ConnectionReference(dbFile, nativeConnection);
            }
        }

        internal void ReturnConnectionReference(ConnectionReference connectionReference)
        {
            var nativeConnection = connectionReference.NativeConnection;
            var fileRefCounter = connectionReference.FileRefCounter;

            nativeConnection.Dispose();

            lock (connections)
            {
                var current = Interlocked.Decrement(ref fileRefCounter.ConnectionCount);

                if (current < 0)
                {
                    throw new InvalidOperationException($"{fileRefCounter.FileName} has been returned too many times");
                }

                if (current == 0)
                {
                    fileRefCounter.Database.Dispose();
                    connections.Remove(fileRefCounter.FileName);
                }
            }
        }
    }
}