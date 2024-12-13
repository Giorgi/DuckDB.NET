using DuckDB.NET.Data.Extensions;
using System;
using System.Data;
using System.Data.Common;

namespace DuckDB.NET.Data;

public class DuckDBTransaction : DbTransaction
{
    private bool finished = false;
    private readonly DuckDBConnection connection;
        
    protected override DbConnection DbConnection => connection;
        
    public override IsolationLevel IsolationLevel { get; }

    public DuckDBTransaction(DuckDBConnection connection, IsolationLevel isolationLevel)
    {
        this.connection = connection;
        IsolationLevel = isolationLevel;

        if (isolationLevel != IsolationLevel.Snapshot && isolationLevel != IsolationLevel.Unspecified)
        {
            throw new ArgumentException($"Unsupported isolation level: {isolationLevel}", nameof(isolationLevel));
        }

        this.connection.ExecuteNonQuery("BEGIN TRANSACTION;");
    }

    public override void Commit() => FinishTransaction("COMMIT;");

    public override void Rollback() => FinishTransaction("ROLLBACK");

    private void FinishTransaction(string finalizer)
    {
        if (finished)
        {
            throw new InvalidOperationException("Transaction has already been finished.");
        }

        connection.ExecuteNonQuery(finalizer);
        connection.Transaction = null;
        finished = true;
    }
        
        
    protected override void Dispose(bool disposing)
    {
        base.Dispose(disposing);

        if (disposing && !finished && connection.IsOpen())
        {
            Rollback();
        }
    }
}