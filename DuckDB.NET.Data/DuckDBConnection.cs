using DuckDB.NET.Data.Internal;
using System;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data
{
    public class DuckDBConnection : DbConnection
    {
        private ConnectionManager connectionManager = ConnectionManager.Default;
        private ConnectionReference connectionReference;
        private ConnectionState connectionState = ConnectionState.Closed;

        internal DbTransaction? Transaction { get; set; }

        public DuckDBConnection(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public override string ConnectionString { get; set; }

        public override string Database { get; }

        public override string DataSource { get; }

        public DuckDBNativeConnection NativeConnection => connectionReference.NativeConnection;

        public override string ServerVersion { get; }

        public override ConnectionState State => connectionState;

        internal ConnectionManager ConnectionManager => connectionManager;

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

            Dispose(true);
        }

        public override void Open()
        {
            if (connectionState == ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection is already open.");
            }

            connectionReference = connectionManager.GetConnectionReference(ConnectionString);

            connectionState = ConnectionState.Open;
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            EnsureConnectionOpen();
            if (Transaction != null)
                throw new InvalidOperationException("Already in a transaction.");
            return Transaction = new DuckDBTransaction(this, isolationLevel);
        }

        protected override DbCommand CreateDbCommand()
        {
            return new DuckDbCommand { Connection = this };
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
                throw new InvalidOperationException($"{operation} requires an open connection");
        }
    }
}