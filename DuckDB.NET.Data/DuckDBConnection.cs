using System;
using System.Data;
using System.Data.Common;

namespace DuckDB.NET.Data
{
    public class DuckDBConnection : DbConnection
    {
        //private string filename;
        //private bool inMemory;
        private ConnectionState connectionState = ConnectionState.Closed;

        private DuckDb pDuckDb;
        internal DuckDBNativeConnection NativeConnection;

        //public DuckDBConnection(string connectionString)
        //{
        //    ConnectionString = connectionString;

        //    if (ConnectionString.StartsWith("Data Source=") || ConnectionString.StartsWith("DataSource="))
        //    {
        //        var strings = ConnectionString.Split('=');

        //        if (strings[1] == ":memory:")
        //        {
        //            inMemory = true;
        //        }
        //        else
        //        {
        //            filename = strings[1];
        //        }
        //    }
        //}

        public DuckDBConnection(DuckDb duckDb)
        {
            pDuckDb = duckDb;
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
            //duckDBDatabase.Dispose();
            connectionState = ConnectionState.Closed;
        }

        public override void Open()
        {
            if (connectionState == ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection is already open.");
            }

            //var result = PlatformIndependentBindings.NativeMethods.DuckDBOpen(inMemory ? null : filename, out duckDBDatabase);
            //if (result.IsSuccess())
            //{
            //    result = PlatformIndependentBindings.NativeMethods.DuckDBConnect(duckDBDatabase, out NativeConnection);

            //    if (!result.IsSuccess())
            //    {
            //        duckDBDatabase.Dispose();
            //        throw new DuckDBException("DuckDBConnect failed", result);
            //    }
            //}
            //else
            //{
            //    throw new DuckDBException("DuckDBOpen failed", result);
            //}

            var result = PlatformIndependentBindings.NativeMethods.DuckDBConnect(pDuckDb.Database, out NativeConnection);
            if (!result.IsSuccess())
            {
                //duckDBDatabase.Dispose();
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
    }
}
