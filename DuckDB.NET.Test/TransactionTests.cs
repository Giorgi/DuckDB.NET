namespace DuckDB.NET.Test;

public class TransactionTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void SimpleTransactionTest()
    {
        Command.CommandText = "CREATE TABLE transactionUsers (id INTEGER, name TEXT);";
        Command.ExecuteNonQuery();

        object rowsInTable;
        using (var transaction = Connection.BeginTransaction(IsolationLevel.Snapshot))
        {
            transaction.IsolationLevel.Should().Be(IsolationLevel.Snapshot);
            transaction.Connection.Should().Be(Connection);

            Command.CommandText = "INSERT INTO transactionUsers VALUES (1, 'user1'), (2, 'user2')";
            Command.ExecuteNonQuery();

            Command.CommandText = "SELECT count(*) FROM transactionUsers";
            rowsInTable = Command.ExecuteScalar();
            rowsInTable.Should().Be(2);
            transaction.Commit();
        }

        Command.CommandText = "SELECT count(*) FROM transactionUsers";
        rowsInTable = Command.ExecuteScalar();
        rowsInTable.Should().Be(2);

        using (Connection.BeginTransaction())
        {
            Command.CommandText = "INSERT INTO transactionUsers VALUES (3, 'user3'), (4, 'user4')";
            Command.ExecuteNonQuery();

            Command.CommandText = "SELECT count(*) FROM transactionUsers";
            rowsInTable = Command.ExecuteScalar();
            rowsInTable.Should().Be(4);
        }

        Command.CommandText = "SELECT count(*) FROM transactionUsers";
        rowsInTable = Command.ExecuteScalar();
        rowsInTable.Should().Be(2);
    }

    [Fact]
    public void ParallelTransactionsTest()
    {
        using (Connection.BeginTransaction())
        {
            Connection
                .Invoking(con => con.BeginTransaction())
                .Should().Throw<InvalidOperationException>();
        }
    }

    [Fact]
    public void CommitTransactionTwiceTest()
    {
        using (var transaction = Connection.BeginTransaction())
        {
            transaction.Commit();
            transaction.Invoking(tr => tr.Commit())
                .Should().Throw<InvalidOperationException>();
        }
    }

    [Fact]
    public void RollbackAndCommitTransactionTest()
    {
        using (var transaction = Connection.BeginTransaction())
        {
            transaction.Rollback();
            transaction.Invoking(tr => tr.Commit())
                .Should().Throw<InvalidOperationException>();
        }
    }

    [Fact]
    public void RollbackTransactionTwiceTest()
    {
        using (var transaction = Connection.BeginTransaction())
        {
            transaction.Rollback();
            transaction.Invoking(tr => tr.Rollback())
                .Should().Throw<InvalidOperationException>();
        }
    }

    [Fact]
    public void CommitAndRollbackTransactionTest()
    {
        using (var transaction = Connection.BeginTransaction())
        {
            transaction.Commit();
            transaction.Invoking(tr => tr.Rollback())
                .Should().Throw<InvalidOperationException>();
        }
    }

    [Fact]
    public void TransactionInvalidStateTest()
    {
        Connection.Close();
        Connection.Invoking(connection => connection.BeginTransaction()).Should().Throw<InvalidOperationException>();
        Connection.Open();

        Connection.Invoking(connection => connection.BeginTransaction(IsolationLevel.Serializable)).Should()
            .Throw<ArgumentException>();
    }

    [Fact(Skip = "Failing for DuckDB 1.5.0")]
    public void AbortedTransactionTest()
    {
        // This block of code is to make the transaction commit fail using an index limitation in duckdb
        // (https://github.com/duckdb/duckdb/issues/17802)
        Command.CommandText = "CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY);";
        Command.ExecuteNonQuery();
        Command.CommandText = "INSERT OR IGNORE INTO test_table VALUES (1);";
        Command.ExecuteNonQuery();
        // Keep a reference to the row in an open transaction to trigger the concurrent access limitation
        using var tx1 = Connection.BeginTransaction();
        Command.Transaction = tx1;
        Command.CommandText = "SELECT id FROM test_table LIMIT 1;";
        using var reader1 = Command.ExecuteReader();
        using var conn2 = Connection.Duplicate();
        conn2.Open();
        using var cmd2 = conn2.CreateCommand();
        cmd2.CommandText = "UPDATE test_table SET id = 1 WHERE id = 1;";
        cmd2.ExecuteNonQuery();
        var tx2 = conn2.BeginTransaction();
        cmd2.Transaction = tx2;
        cmd2.CommandText = "UPDATE test_table SET id = 1 WHERE id = 1;";
        cmd2.ExecuteNonQuery();

        using (new FluentAssertions.Execution.AssertionScope())
        {
            // Check that when the transaction commit fails and the transaction
            // enters an aborted state, the transaction and connection objects
            // remain in the expected state.
            tx2.Invoking(tx2 => tx2.Commit()).Should().Throw<DuckDBException>().Where(ex => ex.ErrorType == Native.DuckDBErrorType.Transaction);
            tx2.Invoking(tx2 => tx2.Commit()).Should().Throw<InvalidOperationException>();
            tx2.Invoking(tx2 => tx2.Rollback()).Should().Throw<InvalidOperationException>();
            tx2.Invoking(tx2 => tx2.Dispose()).Should().NotThrow();

            conn2.Invoking(conn2 => conn2.BeginTransaction()).Should().NotThrow();
        }
    }
}
