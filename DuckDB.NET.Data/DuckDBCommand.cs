using System;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data
{
    public class DuckDbCommand : DbCommand
    {
        private DuckDBConnection connection;
        private readonly DuckDBDbParameterCollection parameters = new DuckDBDbParameterCollection();
        
        internal DuckDBNativeConnection DBNativeConnection => connection.NativeConnection;

        protected override DbParameterCollection DbParameterCollection => parameters;
        protected override DbTransaction DbTransaction { get; set; }
        public override bool DesignTimeVisible { get; set; }

        public DuckDbCommand()
        {
        }

        public DuckDbCommand(string commandText)
        {
            CommandText = commandText;
        }

        public DuckDbCommand(string commandText, DuckDBConnection connection) : this(commandText)
        {
            Connection = connection;
        }

        public override void Cancel()
        {

        }

        public override int ExecuteNonQuery()
        {
            EnsureConnectionOpen();
            using var queryResult = DuckDBStatementExecutor.Execute(connection.NativeConnection, CommandText, parameters);
            return (int)NativeMethods.Query.DuckDBRowsChanged(queryResult.NativeHandle);
        }

        public override object ExecuteScalar()
        {
            EnsureConnectionOpen();
            
            using var reader = ExecuteReader();
            return reader.Read() ? reader.GetValue(0) : null;
        }

        public override void Prepare()
        {
            throw new NotImplementedException();
        }

        public override string CommandText { get; set; }
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }

        protected override DbConnection DbConnection
        {
            get => connection;
            set => connection = (DuckDBConnection)value;
        }
        
        protected override DbParameter CreateDbParameter() => new DuckDBParameter();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            EnsureConnectionOpen();
            return new DuckDBDataReader(this, behavior, parameters);
        }

        internal void CloseConnection()
        {
            Connection.Close();
        }

        private void EnsureConnectionOpen([CallerMemberName]string operation = "")
        {
            if (connection.State != ConnectionState.Open)
            {
                throw new InvalidOperationException($"{operation} requires an open connection");
            }
        }
    }
}
