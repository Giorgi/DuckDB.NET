using DuckDB.NET.Data.Internal;
using System;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using DuckDB.NET.Data.ConnectionString;

namespace DuckDB.NET.Data
{
    public class DuckDBConnection : DbConnection
    {
        private readonly ConnectionManager connectionManager = ConnectionManager.Default;
        private ConnectionReference connectionReference;
        private ConnectionState connectionState = ConnectionState.Closed;

        internal DuckDBTransaction Transaction { get; set; }

        public DuckDBConnection(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public override string ConnectionString { get; set; }

        public override string Database { get; }

        public override string DataSource { get; }

        internal DuckDBNativeConnection NativeConnection => connectionReference.NativeConnection;

        public override string ServerVersion => NativeMethods.Startup.DuckDBLibraryVersion().ToManagedString(false);

        public override ConnectionState State => connectionState;

        public override void ChangeDatabase(string databaseName)
        {
            throw new NotSupportedException();
        }

        public override void Close()
        {
            if (connectionState == ConnectionState.Closed)
            {
                throw new InvalidOperationException("Connection is already closed.");
            }

            Dispose(true);
        }

        public override void Open()
        {
            if (connectionState == ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection is already open.");
            }

            var connectionString = DuckDBConnectionStringParser.Parse(ConnectionString);

            connectionReference = connectionManager.GetConnectionReference(connectionString);

            connectionState = ConnectionState.Open;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            return BeginTransaction(isolationLevel);
        }

        public new DuckDBTransaction BeginTransaction()
        {
            return BeginTransaction(IsolationLevel.Unspecified);
        }

        private new DuckDBTransaction BeginTransaction(IsolationLevel isolationLevel)
        {
            EnsureConnectionOpen();
            if (Transaction != null)
            {
                throw new InvalidOperationException("Already in a transaction.");
            }

            return Transaction = new DuckDBTransaction(this, isolationLevel);
        }

        protected override DbCommand CreateDbCommand()
        {
            return CreateCommand();
        }

        public new virtual DuckDbCommand CreateCommand()
        {
            return new DuckDbCommand
            {
                Connection = this,
                Transaction = Transaction
            };
        }

        public DuckDBAppender CreateAppender(string table)
        {
            if (NativeMethods.Appender.DuckDBAppenderCreate(NativeConnection, null, table, out var nativeAppender) == DuckDBState.DuckDBError)
            {
                DuckDBAppender.ThrowLastError(nativeAppender);
            }

            return new DuckDBAppender(nativeAppender);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (connectionState == ConnectionState.Open)
                {
                    connectionManager.ReturnConnectionReference(connectionReference);
                    connectionState = ConnectionState.Closed;
                }
            }

            base.Dispose(disposing);
        }
        
        private void EnsureConnectionOpen([CallerMemberName]string operation = "")
        {
            if (State != ConnectionState.Open)
            {
                throw new InvalidOperationException($"{operation} requires an open connection");
            }
        }
    }
}