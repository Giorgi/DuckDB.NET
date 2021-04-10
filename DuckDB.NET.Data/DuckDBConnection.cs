using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Threading;

namespace DuckDB.NET.Data
{
    public class DuckDBConnection : DbConnection
    {
        private static ConcurrentDictionary<string, DatabaseFile> databaseConnections = new(StringComparer.OrdinalIgnoreCase);

        private readonly bool inMemory;
        private readonly string filename;

        private ConnectionState connectionState = ConnectionState.Closed;

        internal DuckDBNativeConnection NativeConnection;

        public DuckDBConnection(string connectionString)
        {
            ConnectionString = connectionString;

            if (ConnectionString.StartsWith("Data Source=") || ConnectionString.StartsWith("DataSource="))
            {
                var strings = ConnectionString.Split('=');

                if (strings[1] == ":memory:")
                {
                    inMemory = true;
                    filename = "";
                }
                else
                {
                    filename = strings[1];
                }
            }
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new NotImplementedException();
        }

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotImplementedException();
        }

        public override void Close()
        {
            if (connectionState == ConnectionState.Closed)
            {
                throw new InvalidOperationException("Connection is already closed.");
            }

            NativeConnection.Dispose();

            if (databaseConnections.TryGetValue(filename, out var dbFile))
            {
                Interlocked.Decrement(ref dbFile.Count);
                if (dbFile.Count == 0)
                {
                    dbFile.Database.Dispose();
                    databaseConnections.TryRemove(filename, out dbFile);
                }
            }

            connectionState = ConnectionState.Closed;
        }

        public override void Open()
        {
            if (connectionState == ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection is already open.");
            }

            DuckDBState result;

            databaseConnections.TryGetValue(filename, out var dbFile);

            if (dbFile == null)
            {
                result = PlatformIndependentBindings.NativeMethods.DuckDBOpen(inMemory ? null : filename, out var duckDBDatabase);
                if (!result.IsSuccess())
                {
                    throw new DuckDBException("DuckDBOpen failed", result);
                }

                dbFile = new DatabaseFile { Database = duckDBDatabase };
                dbFile = databaseConnections.AddOrUpdate(filename, dbFile, (s, file) => file);
            }

            result = PlatformIndependentBindings.NativeMethods.DuckDBConnect(dbFile.Database, out NativeConnection);

            if (result.IsSuccess())
            {
                Interlocked.Increment(ref dbFile.Count);
            }
            else
            {
                throw new DuckDBException("DuckDBConnect failed", result);
            }

            connectionState = ConnectionState.Open;
        }

        public override string ConnectionString { get; set; }

        public override string Database { get; }

        public override ConnectionState State => connectionState;

        public override string DataSource { get; }

        public override string ServerVersion { get; }

        protected override DbCommand CreateDbCommand()
        {
            return new DuckDbCommand { Connection = this };
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Close();
            }

            base.Dispose(disposing);
        }
    }

    class DatabaseFile
    {
        public int Count;
        public DuckDBDatabase Database { get; set; }
    }
}
