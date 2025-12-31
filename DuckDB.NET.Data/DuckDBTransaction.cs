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

        try
        {
            connection.ExecuteNonQuery(finalizer);
            Cleanup();
        }
        // If something goes wrong with the transaction, to match the
        // transaction's internal duckdb state it should still be considered
        // finished and should no longer be used
        catch (DuckDBException ex) when (ex.ErrorType == Native.DuckDBErrorType.Transaction)
        {
            Cleanup();
            throw;
        }

        void Cleanup()
        {
            connection.Transaction = null;
            finished = true;
        }
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
