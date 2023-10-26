using System;
using System.Data;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class TransactionTests : DuckDBTestBase
{
    public TransactionTests(DuckDBDatabaseFixture db) : base(db)
    {
    }

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
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Invoking(connection => connection.BeginTransaction()).Should().Throw<InvalidOperationException>();
        connection.Open();

        connection.Invoking(connection => connection.BeginTransaction(IsolationLevel.Serializable)).Should()
            .Throw<ArgumentException>();
    }
}