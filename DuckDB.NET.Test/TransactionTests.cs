using System;
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

            command.CommandText = "CREATE TABLE users (id INTEGER, name TEXT);";
            command.ExecuteNonQuery();

            object rowsInTable;
            using (var transaction = connection.BeginTransaction())
            {
                command.CommandText = "INSERT INTO users VALUES (1, 'user1'), (2, 'user2')";
                command.ExecuteNonQuery();

                command.CommandText = "SELECT count(*) FROM users";
                rowsInTable = command.ExecuteScalar();
                rowsInTable.Should().Be(2);
                transaction.Commit();
            }
            
            command.CommandText = "SELECT count(*) FROM users";
            rowsInTable = command.ExecuteScalar();
            rowsInTable.Should().Be(2);
            
            using (connection.BeginTransaction())
            {
                command.CommandText = "INSERT INTO users VALUES (3, 'user3'), (4, 'user4')";
                command.ExecuteNonQuery();

                command.CommandText = "SELECT count(*) FROM users";
                rowsInTable = command.ExecuteScalar();
                rowsInTable.Should().Be(4);
            }
            
            command.CommandText = "SELECT count(*) FROM users";
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
    }
}