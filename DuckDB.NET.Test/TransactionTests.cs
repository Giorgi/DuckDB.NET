using System;
using System.Data;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test
{
    public class TransactionTests
    {
        [Fact]
        public void SimpleTransactionTest()
        {
            using var connection = new DuckDBConnection("DataSource=:memory:");
            connection.Open();

            var command = connection.CreateCommand();

            command.CommandText = "CREATE TABLE transactionUsers (id INTEGER, name TEXT);";
            command.ExecuteNonQuery();

            object rowsInTable;
            using (var transaction = connection.BeginTransaction(IsolationLevel.Snapshot))
            {
                transaction.IsolationLevel.Should().Be(IsolationLevel.Snapshot);
                transaction.Connection.Should().Be(connection);

                command.CommandText = "INSERT INTO transactionUsers VALUES (1, 'user1'), (2, 'user2')";
                command.ExecuteNonQuery();

                command.CommandText = "SELECT count(*) FROM transactionUsers";
                rowsInTable = command.ExecuteScalar();
                rowsInTable.Should().Be(2);
                transaction.Commit();
            }

            command.CommandText = "SELECT count(*) FROM transactionUsers";
            rowsInTable = command.ExecuteScalar();
            rowsInTable.Should().Be(2);

            using (connection.BeginTransaction())
            {
                command.CommandText = "INSERT INTO transactionUsers VALUES (3, 'user3'), (4, 'user4')";
                command.ExecuteNonQuery();

                command.CommandText = "SELECT count(*) FROM transactionUsers";
                rowsInTable = command.ExecuteScalar();
                rowsInTable.Should().Be(4);
            }

            command.CommandText = "SELECT count(*) FROM transactionUsers";
            rowsInTable = command.ExecuteScalar();
            rowsInTable.Should().Be(2);
        }

        [Fact]
        public void ParallelTransactionsTest()
        {
            using var connection = new DuckDBConnection("DataSource=:memory:");
            connection.Open();

            using (connection.BeginTransaction())
            {
                connection.Invoking(con => con.BeginTransaction())
                    .Should().Throw<InvalidOperationException>();
            }
        }

        [Fact]
        public void CommitTransactionTwiceTest()
        {
            using var connection = new DuckDBConnection("DataSource=:memory:");
            connection.Open();

            using (var transaction = connection.BeginTransaction())
            {
                transaction.Commit();
                transaction.Invoking(tr => tr.Commit())
                    .Should().Throw<InvalidOperationException>();
            }
        }

        [Fact]
        public void RollbackAndCommitTransactionTest()
        {
            using var connection = new DuckDBConnection("DataSource=:memory:");
            connection.Open();

            using (var transaction = connection.BeginTransaction())
            {
                transaction.Rollback();
                transaction.Invoking(tr => tr.Commit())
                    .Should().Throw<InvalidOperationException>();
            }
        }

        [Fact]
        public void RollbackTransactionTwiceTest()
        {
            using var connection = new DuckDBConnection("DataSource=:memory:");
            connection.Open();

            using (var transaction = connection.BeginTransaction())
            {
                transaction.Rollback();
                transaction.Invoking(tr => tr.Rollback())
                    .Should().Throw<InvalidOperationException>();
            }
        }

        [Fact]
        public void CommitAndRollbackTransactionTest()
        {
            using var connection = new DuckDBConnection("DataSource=:memory:");
            connection.Open();

            using (var transaction = connection.BeginTransaction())
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
}