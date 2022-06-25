using DuckDB.NET.Data.Internal;
using System;
using System.Data;
using System.Data.Common;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data
{
    public class DuckDbCommand : DbCommand
    {
        private DuckDBConnection connection;

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
            return ExecuteScalarOrNonQuery();
        }

        public override object ExecuteScalar()
        {
            return ExecuteScalarOrNonQuery();
        }

        private int ExecuteScalarOrNonQuery()
        {
            using var unmanagedString = CommandText.ToUnmanagedString();
            var queryResult = new DuckDBResult();
            var result = PlatformIndependentBindings.NativeMethods.DuckDBQuery(connection.NativeConnection, unmanagedString, queryResult);

            if (!string.IsNullOrEmpty(queryResult.ErrorMessage))
            {
                throw new DuckDBException(queryResult.ErrorMessage, result);
            }

            if (!result.IsSuccess())
            {
                throw new DuckDBException("DuckDBQuery failed", result);
            }

            if (queryResult.ColumnCount > 0 && queryResult.RowCount > 0)
            {
                return PlatformIndependentBindings.NativeMethods.DuckDBValueInt32(queryResult, 0, 0);
            }

            return 0;
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

        internal DuckDBNativeConnection DBNativeConnection => connection.NativeConnection;

        protected override DbParameterCollection DbParameterCollection { get; }
        protected override DbTransaction DbTransaction { get; set; }
        public override bool DesignTimeVisible { get; set; }

        protected override DbParameter CreateDbParameter()
        {
            throw new NotImplementedException();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return new DuckDBDataReader(this, behavior);
        }

        internal void CloseConnection()
        {
            Connection.Close();
        }
    }
}
