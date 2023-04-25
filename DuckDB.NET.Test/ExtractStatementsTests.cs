using System;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class ExtractStatementsTests
{
    private readonly DuckDBConnection connection;

    public ExtractStatementsTests()
    {
        connection = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
        connection.Open();
    }

    [Fact]
    public void MultipleInsertsIntoTable()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE Test(foo INTEGER, bar INTEGER);";
        command.ExecuteNonQuery();

        command.CommandText = "Insert into Test (foo, bar) values (1,2); Insert into Test (foo, bar) values (3,4);";
        command.ExecuteNonQuery().Should().Be(2);

        command.CommandText = "Select * from Test";
        var dataReader = command.ExecuteReader();

        dataReader.Read();
        dataReader.GetInt32(0).Should().Be(1);
        dataReader.GetInt32(1).Should().Be(2);
        
        dataReader.Read();
        dataReader.GetInt32(0).Should().Be(3);
        dataReader.GetInt32(1).Should().Be(4);
    }

    [Fact]
    public void WrongCommandThrowsException()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "error";
        
        command.Invoking(cmd => cmd.ExecuteNonQuery()).Should()
               .Throw<DuckDBException>().Where(e => e.Message.Contains("syntax error at or near"));
    }

    [Fact]
    public void NotExistingTableThrowsException()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "Select 2; Select 1 from dummy";
        
        command.Invoking(cmd => cmd.ExecuteNonQuery()).Should()
               .Throw<DuckDBException>().Where(e => e.Message.Contains("Table with name dummy does not exist"));
    }

    [Fact]
    public void MissingParametersThrowsException()
    {
        using var command = connection.CreateCommand();
        command.CommandText = "Select ?1; Select ?1, ?2";
        command.Parameters.Add(new DuckDBParameter(42));

        command.Invoking(cmd => cmd.ExecuteReader()).Should()
            .Throw<InvalidOperationException>().Where(e => e.Message.Contains("Invalid number of parameters. Expected 2, got 1"));
    }
}