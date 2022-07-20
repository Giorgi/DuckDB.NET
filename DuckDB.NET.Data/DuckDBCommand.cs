using System;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data
{
    public class DuckDbCommand : DbCommand
    {
        private DuckDBConnection connection;
        private readonly DuckDBDbParameterCollection parameters = new();

        private string commandText;
        private PreparedStatement preparedStatement;

        public override bool DesignTimeVisible { get; set; }
        protected override DbTransaction DbTransaction { get; set; }
        protected override DbParameterCollection DbParameterCollection => parameters;

        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }

        public override string CommandText
        {
            get => commandText;
            set
            {
                commandText = value;
                preparedStatement?.Dispose();
                preparedStatement = null;
            }
        }

        protected override DbConnection DbConnection
        {
            get => connection;
            set => connection = (DuckDBConnection)value;
        }

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

            PrepareIfNeeded();

            using var queryResult = preparedStatement.Execute(parameters);
            return (int)NativeMethods.Query.DuckDBRowsChanged(queryResult);
        }

        public override object ExecuteScalar()
        {
            EnsureConnectionOpen();

            using var reader = ExecuteReader();
            return reader.Read() ? reader.GetValue(0) : null;
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            EnsureConnectionOpen();

            PrepareIfNeeded();

            var queryResult = preparedStatement.Execute(parameters);
            var reader = new DuckDBDataReader(this, queryResult, behavior);

            return reader;
        }

        public override void Prepare() => PrepareIfNeeded();

        protected override DbParameter CreateDbParameter() => new DuckDBParameter();

        protected override void Dispose(bool disposing)
        {
            preparedStatement?.Dispose();
        }

        private void PrepareIfNeeded()
        {
            preparedStatement ??= PreparedStatement.Prepare(connection.NativeConnection, CommandText);
        }

        internal void CloseConnection()
        {
            Connection.Close();
        }

        private void EnsureConnectionOpen([CallerMemberName] string operation = "")
        {
            if (connection.State != ConnectionState.Open)
            {
                throw new InvalidOperationException($"{operation} requires an open connection");
            }
        }
    }
}
