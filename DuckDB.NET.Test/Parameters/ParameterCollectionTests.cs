using System;
using System.Collections.Generic;
using System.Data;
using Dapper;
using DuckDB.NET.Data;
using DuckDB.NET.Test.Helpers;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class ParameterCollectionTests
{
    [Theory]
    [InlineData("SELECT ?1;")]
    [InlineData("SELECT ?;")]
    [InlineData("SELECT $1;")]
    public void BindSingleValueTest(string query)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var command = connection.CreateCommand();

        command.Parameters.Add(new DuckDBParameter("1", 42));
        command.CommandText = query;
        var scalar = command.ExecuteScalar();
        scalar.Should().Be(42);
    }
    
    [Theory]
    [InlineData("SELECT ?1;")]
    [InlineData("SELECT ?;")]
    [InlineData("SELECT $1;")]
    public void BindSingleValueNullTest(string query)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var command = connection.CreateCommand();

        command.Parameters.Add(new DuckDBParameter("1", null));
        command.CommandText = query;
        var scalar = command.ExecuteScalar();
        scalar.Should().Be(DBNull.Value);
    }

    [Fact]
    public void BindNullValueTest()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var command = new DuckDbCommand("Select ?", connection);
        command.Parameters.Add(new DuckDBParameter());

        var scalar = command.ExecuteScalar();
        scalar.Should().Be(DBNull.Value);
    }

    [Fact]
    public void ParameterCountMismatchTest()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var command = new DuckDbCommand("Select ?", connection);

        command.Invoking(dbCommand => dbCommand.ExecuteScalar()).Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void PrepareCommandNoOperationTest()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var command = new DuckDbCommand("Select ? from nowhere", connection);

        command.Invoking(dbCommand => dbCommand.Prepare()).Should().NotThrow();
    }

    [Fact]
    public void ParameterConstructorTests()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "CREATE TABLE ParameterConstructorTests (key INTEGER, value double, State Boolean, ErrorCode Long)";
        duckDbCommand.ExecuteNonQuery();

        duckDbCommand.CommandText = "Insert Into ParameterConstructorTests values (?,?,?,?)";
        duckDbCommand.Parameters.Add(new DuckDBParameter(DbType.Double, 2.4));
        duckDbCommand.Parameters.Add(new DuckDBParameter(true));
        duckDbCommand.Parameters.Insert(0, new DuckDBParameter(2));
        duckDbCommand.Parameters.RemoveAt(2);
        duckDbCommand.Parameters.AddRange(new List<DuckDBParameter>{new()
        {
            Value = true
        }, new(24)});

        duckDbCommand.ExecuteNonQuery();

        duckDbCommand.CommandText = "select * from ParameterConstructorTests";
        duckDbCommand.Parameters.Clear();

        var dataReader = duckDbCommand.ExecuteReader();
        dataReader.Read();

        dataReader.GetInt32(0).Should().Be(2);
        dataReader.GetDouble(1).Should().Be(2.4);
        dataReader.GetBoolean(2).Should().Be(true);
        dataReader.GetInt32(3).Should().Be(24);
    }

    [Fact]
    public void DuckDBParameterCollectionTests()
    {
        var parameters = new DuckDBParameterCollection();
        var duckDBParameterDouble = new DuckDBParameter(2.5);
        parameters.Add(duckDBParameterDouble);

        parameters.Contains(duckDBParameterDouble).Should().BeTrue();
        parameters.IndexOf(duckDBParameterDouble).Should().Be(0);
        parameters.Remove(duckDBParameterDouble);
        parameters.Contains(duckDBParameterDouble).Should().BeFalse();

        parameters.Add(duckDBParameterDouble);

        var duckDBParameterLong = new DuckDBParameter("param0", 5L);
        parameters[0] = duckDBParameterLong;
        parameters.Contains(duckDBParameterLong).Should().BeTrue();

        var duckDBParameterFloat = new DuckDBParameter("param1",2f);
        parameters["param0"] = duckDBParameterFloat;
        parameters["param1"].Should().Be(duckDBParameterFloat);

        parameters.RemoveAt("param1");
        parameters.Count.Should().Be(0);
    }

    [Theory]
    [InlineData("INSERT INTO ParametersTestKeyValue (KEY, VALUE) VALUES (?, ?)")]
    [InlineData("INSERT INTO ParametersTestKeyValue (KEY, VALUE) VALUES (?1, ?2)")]
    [InlineData("INSERT INTO ParametersTestKeyValue (KEY, VALUE) VALUES ($1, $2)")]
    [InlineData("UPDATE ParametersTestKeyValue SET KEY = ?, VALUE = ?;")]
    [InlineData("UPDATE ParametersTestKeyValue SET KEY = ?1, VALUE = ?2;")]
    [InlineData("UPDATE ParametersTestKeyValue SET KEY = $1, VALUE = $2;")]
    public void BindMultipleValuesTest(string queryStatement)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        using var defer = new Defer(() => connection.Execute("DROP TABLE ParametersTestKeyValue;"));
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE ParametersTestKeyValue (KEY INTEGER, VALUE TEXT)";
        command.ExecuteNonQuery();
        command.CommandText = "INSERT INTO ParametersTestKeyValue (KEY, VALUE) VALUES (42, 'test string');";
        command.ExecuteNonQuery();

        command.CommandText = queryStatement;
        command.Parameters.Add(new DuckDBParameter("1", 42));
        command.Parameters.Add(new DuckDBParameter("2", "hello"));
        var affectedRows = command.ExecuteNonQuery();
        affectedRows.Should().NotBe(0);
    }

    [Theory]
    [InlineData("INSERT INTO ParametersTestKeyValue (KEY, VALUE) VALUES ($key, $value)")]
    [InlineData("UPDATE ParametersTestKeyValue SET KEY = $key, VALUE = $value;")]
    public void BindMultipleValuesTestNamedParameters(string queryStatement)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        using var defer = new Defer(() => connection.Execute("DROP TABLE ParametersTestKeyValue;"));
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE ParametersTestKeyValue (KEY INTEGER, VALUE TEXT)";
        command.ExecuteNonQuery();
        command.CommandText = "INSERT INTO ParametersTestKeyValue (KEY, VALUE) VALUES (42, 'test string');";
        command.ExecuteNonQuery();

        command.CommandText = queryStatement;
        command.Parameters.Add(new DuckDBParameter("key", 42));
        command.Parameters.Add(new DuckDBParameter("value", "hello"));
        var affectedRows = command.ExecuteNonQuery();
        affectedRows.Should().NotBe(0);
    }

    [Theory]
    [InlineData("INSERT INTO ParametersTestInvalidOrderKeyValue (KEY, VALUE) VALUES (?2, ?1)")]
    [InlineData("INSERT INTO ParametersTestInvalidOrderKeyValue (KEY, VALUE) VALUES ($2, $1)")]
    [InlineData("UPDATE ParametersTestInvalidOrderKeyValue SET Key = ?2, Value = ?1;")]
    [InlineData("UPDATE ParametersTestInvalidOrderKeyValue SET Key = $2, Value = $1;")]
    public void BindMultipleValuesInvalidOrderTest(string queryStatement)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        using var defer = new Defer(() => connection.Execute("DROP TABLE ParametersTestInvalidOrderKeyValue;"));
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = "CREATE TABLE ParametersTestInvalidOrderKeyValue (KEY INTEGER, VALUE TEXT)";
        command.ExecuteNonQuery();
        command.CommandText = "INSERT INTO ParametersTestInvalidOrderKeyValue (KEY, VALUE) VALUES (42, 'test string');";
        command.ExecuteNonQuery();

        command.CommandText = queryStatement;
        command.Parameters.Add(new DuckDBParameter("param1", 42));
        command.Parameters.Add(new DuckDBParameter("param2", "hello"));
        command.Invoking(cmd => cmd.ExecuteNonQuery())
            .Should().ThrowExactly<InvalidOperationException>();

        command.Parameters.Clear();
        command.Parameters.Add(new DuckDBParameter(42));
        command.Parameters.Add(new DuckDBParameter("hello"));
        command.Invoking(cmd => cmd.ExecuteNonQuery())
            .Should().ThrowExactly<DuckDBException>();
    }

    [Theory]
    // Dapper supports ? placeholders when using both DynamicParameters and an object
    [InlineData("INSERT INTO DapperParametersObjectBindingTest VALUES (?, ?);")]
    [InlineData("UPDATE DapperParametersObjectBindingTest SET a = ?, b = ?;")]
    public void BindDapperWithObjectTest(string queryStatement)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        using var defer = new Defer(() => connection.Execute("DROP TABLE DapperParametersObjectBindingTest;"));
        connection.Open();

        connection.Execute("CREATE TABLE DapperParametersObjectBindingTest (a INTEGER, b TEXT);");
        connection.Execute("INSERT INTO DapperParametersObjectBindingTest (a, b) VALUES (42, 'test string');");

        var dp = new DynamicParameters();
        dp.Add("?1", 1);
        dp.Add("?2", "test");

        connection.Execute(queryStatement, dp).Should().BeGreaterOrEqualTo(1);
    }

    [Theory]
    // Dapper supports ? placeholders when using both DynamicParameters and an object
    [InlineData("INSERT INTO DapperParametersObjectBindingTest VALUES ($foo, $bar);")]
    [InlineData("UPDATE DapperParametersObjectBindingTest SET a = $foo, b = $bar;")]
    public void BindDapperWithObjectTestNamesParameters(string queryStatement)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        using var defer = new Defer(() => connection.Execute("DROP TABLE DapperParametersObjectBindingTest;"));
        connection.Open();

        connection.Execute("CREATE TABLE DapperParametersObjectBindingTest (a INTEGER, b TEXT);");
        connection.Execute("INSERT INTO DapperParametersObjectBindingTest (a, b) VALUES (42, 'test string');");

        var dp = new DynamicParameters();
        dp.Add("foo", 1);
        dp.Add("bar", "test");
        
        connection.Execute(queryStatement, dp).Should().BeGreaterOrEqualTo(1);
        connection.Execute(queryStatement, new { foo = 1, bar = "test" }).Should().BeGreaterOrEqualTo(1, "Passing parameters as object should work");
    }

    [Theory]
    // Dapper supports such placeholders when using DynamicParameters
    [InlineData("INSERT INTO DapperParametersDynamicParamsBindingTest VALUES (?1, ?2);")]
    [InlineData("INSERT INTO DapperParametersDynamicParamsBindingTest VALUES ($1, $2);")]
    [InlineData("UPDATE DapperParametersDynamicParamsBindingTest SET a = ?1, b = ?2;")]
    [InlineData("UPDATE DapperParametersDynamicParamsBindingTest SET a = $1, b = $2;")]
    public void BindDapperDynamicParamsOnlyTest(string queryStatement)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        using var defer = new Defer(() => connection.Execute("DROP TABLE DapperParametersDynamicParamsBindingTest;"));
        connection.Open();

        connection.Execute("CREATE TABLE DapperParametersDynamicParamsBindingTest (a INTEGER, b TEXT);");

        var dp = new DynamicParameters();
        dp.Add("1", 1);
        dp.Add("2", "test");

        connection.Execute(queryStatement, dp).Should().BeLessOrEqualTo(1);
    }
    
    [Theory]
    [InlineData("SELECT ?1;")]
    [InlineData("SELECT ?;")]
    [InlineData("SELECT $1;")]
    public void BindSingleValueDapperNullTest(string query)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var parameters = new DynamicParameters();
        parameters.Add("1", null);
        var scalar = connection.QuerySingle<long?>(query, parameters);
        scalar.Should().BeNull();
    }

    [Theory]
    // Dapper does not support such placeholders when using an object :(
    [InlineData("INSERT INTO DapperParametersObjectBindingFailTest VALUES (?1, ?2);")]
    [InlineData("INSERT INTO DapperParametersObjectBindingFailTest VALUES ($1, $2);")]
    [InlineData("UPDATE DapperParametersObjectBindingFailTest SET a = ?1, b = ?2;")]
    [InlineData("UPDATE DapperParametersObjectBindingFailTest SET a = $1, b = $2;")]
    public void BindDapperObjectFailuresTest(string queryStatement)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        using var defer = new Defer(() => connection.Execute("DROP TABLE DapperParametersObjectBindingFailTest;"));
        connection.Open();

        connection.Execute("CREATE TABLE DapperParametersObjectBindingFailTest (a INTEGER, b TEXT);");

        connection.Invoking(con => con.Execute(queryStatement, new { param1 = 1, param2 = "hello" }))
            .Should().ThrowExactly<InvalidOperationException>();
    }
}