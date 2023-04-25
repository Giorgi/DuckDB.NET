using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace DuckDB.NET.Data
{
    public class DuckDbCommand : DbCommand
    {
        private DuckDBConnection connection;
        private readonly DuckDBParameterCollection parameters = new();

        private string commandText;
        private List<PreparedStatement> preparedStatements;

        protected override DbTransaction DbTransaction { get; set; }
        protected override DbParameterCollection DbParameterCollection => parameters;

        public new virtual DuckDBParameterCollection Parameters => parameters;

        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }

        public override string CommandText
        {
            get => commandText;
            set
            {
                commandText = value;
                ReleaseStatements();
                preparedStatements = null;
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

            var result = 0;
            foreach (var statement in preparedStatements)
            {
                using var queryResult = statement.Execute(parameters);
                result += (int)NativeMethods.Query.DuckDBRowsChanged(queryResult);
            }

            return result;
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

            PrepareIfNeeded();

            var results = new List<DuckDBResult>();

            foreach (var statement in preparedStatements)
            {
                try
                {
                    var result = statement.Execute(parameters);
                    results.Add(result);
                }
                catch (Exception ex)
                {
                    var capture = ExceptionDispatchInfo.Capture(ex);

                    foreach (var result in results)
                    {
                        result.Dispose();
                    }

                    capture.Throw();
                }
            }

            var reader = new DuckDBDataReader(this, results, behavior);

            return reader;
        }

        public override void Prepare() => PrepareIfNeeded();

        protected override DbParameter CreateDbParameter() => new DuckDBParameter();

        protected override void Dispose(bool disposing)
        {
            ReleaseStatements();
        }

        private void ReleaseStatements()
        {
            if (preparedStatements != null)
            {
                foreach (var statement in preparedStatements)
                {
                    statement.Dispose();
                }
            }
        }

        private void PrepareIfNeeded()
        {
            preparedStatements ??= PreparedStatement.PrepareMultiple(connection.NativeConnection, CommandText);
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
