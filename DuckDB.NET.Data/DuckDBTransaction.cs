using System;
using System.Data;
using System.Data.Common;
using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Data
{
    internal class DuckDBTransaction : DbTransaction
    {
        private readonly DuckDBConnection _connection;
        protected override DbConnection DbConnection => _connection;
        private bool _finished = false;
        public override IsolationLevel IsolationLevel { get; }

        public DuckDBTransaction(DuckDBConnection connection, IsolationLevel isolationLevel)
        {
            _connection = connection;
            IsolationLevel = isolationLevel;

            if (isolationLevel != IsolationLevel.Serializable && isolationLevel != IsolationLevel.Unspecified)
                throw new ArgumentException($"Unsupported isolation level: {isolationLevel}", nameof(isolationLevel));

            _connection.ExecuteNonQuery("BEGIN TRANSACTION;");
        }

        public override void Commit()
            => FinishTransaction("COMMIT;");

        public override void Rollback()
            => FinishTransaction("ROLLBACK");

        private void FinishTransaction(string finalizer)
        {
            if (_finished)
                throw new InvalidOperationException("Transaction has already been finished.");
            _connection.ExecuteNonQuery(finalizer);
            _connection.Transaction = null;
            _finished = true;
        }
        
        
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            
            if (disposing && !_finished && _connection.IsOpen())
                Rollback();
        }
    }
}