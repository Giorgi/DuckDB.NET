using DuckDB.NET.Data.Internal;
using System;
using System.Data;
using System.Data.Common;

namespace DuckDB.NET.Data
{
    public class DuckDBConnection : DbConnection
    {
        private ConnectionManager connectionManager = ConnectionManager.Default;
        private ConnectionReference connectionReference;
        private ConnectionState connectionState = ConnectionState.Closed;

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
            if (connectionState == ConnectionState.Open)
            {
                connectionManager.ReturnConnectionReference(connectionReference);
                connectionState = ConnectionState.Closed;
            }
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
            throw new NotImplementedException();
        }

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
}