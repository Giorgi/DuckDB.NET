using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data
{
    public class DuckDbCommand : DbCommand
    {
        private DuckDBConnection connection;
        private readonly DuckDBParameterCollection parameters = new();

        protected override DbTransaction DbTransaction { get; set; }
        protected override DbParameterCollection DbParameterCollection => parameters;

        public new virtual DuckDBParameterCollection Parameters => parameters;

        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }

        public override string CommandText { get; set; }

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

            var (preparedStatements, results) = PreparedStatement.PrepareMultiple(connection.NativeConnection, CommandText, parameters);

            var count = 0;

            foreach (var result in results)
            {
                count += (int)NativeMethods.Query.DuckDBRowsChanged(result);

                result.Dispose();
            }

            foreach (var statement in preparedStatements)
            {
                statement.Dispose();
            }

            return count;
        }

        public override object ExecuteScalar()
        {
            EnsureConnectionOpen();

            using var reader = ExecuteReader();
            return reader.Read() ? reader.GetValue(0) : null;
        }

        public new DuckDBDataReader ExecuteReader()
        {
            return (DuckDBDataReader)base.ExecuteReader();
        }

        public new DuckDBDataReader ExecuteReader(CommandBehavior behavior)
        {
            return (DuckDBDataReader)base.ExecuteReader(behavior);
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            EnsureConnectionOpen();

            var (preparedStatements, results) = PreparedStatement.PrepareMultiple(connection.NativeConnection, CommandText, parameters);

            var reader = new DuckDBDataReader(this, results, behavior);

            foreach (var statement in preparedStatements)
            {
                statement.Dispose();
            }

            return reader;
        }

        public override void Prepare() => throw new NotSupportedException("Prepare not supported"); //PrepareIfNeeded();

        protected override DbParameter CreateDbParameter() => new DuckDBParameter();
        
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
