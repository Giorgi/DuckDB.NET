using System;
using System.Data;
using System.Data.Common;

namespace DuckDB.NET.Data
{
    public class DuckDBConnection : DbConnection
    {
        private string filename;
        private bool inMemory;
        private bool connectionIsOpen = false;

        private DuckDBDatabase duckDBDatabase;
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
            NativeConnection.Dispose();
            duckDBDatabase.Dispose();
            connectionIsOpen = false;
        }

        public override void Open()
        {
            if (!connectionIsOpen){
                var result = PlatformIndependentBindings.NativeMethods.DuckDBOpen(inMemory ? null : filename, out duckDBDatabase);
                if (result.IsSuccess())
                {
                    result = PlatformIndependentBindings.NativeMethods.DuckDBConnect(duckDBDatabase, out NativeConnection);

                    if (!result.IsSuccess())
                    {
                        duckDBDatabase.Dispose();
                        throw new DuckDBException("DuckDBConnect failed", result);
                    }
                }
                else
                {
                    throw new DuckDBException("DuckDBOpen failed", result);
                }
                connectionIsOpen = true;
            }
        }

        public override string ConnectionString { get; set; }

        public override string Database { get; }

        public override ConnectionState State { get; }

        public override string DataSource { get; }

        public override string ServerVersion { get; }

        protected override DbCommand CreateDbCommand()
        {
            return new DuckDbCommand { Connection = this };
        }
    }
}
