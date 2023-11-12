using System;
using System.Collections.Generic;
using System.Data;
using Dapper;
using DuckDB.NET.Data;
using DuckDB.NET.Test.Helpers;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class ParameterCollectionTests : DuckDBTestBase
{
	public ParameterCollectionTests(DuckDBDatabaseFixture db) : base(db)
	{
	}

	[Theory]
    [InlineData("SELECT ?1;")]
    [InlineData("SELECT ?;")]
    [InlineData("SELECT $1;")]
    public void BindSingleValueTest(string query)
    {
        Command.Parameters.Add(new DuckDBParameter("1", 42));
		Command.CommandText = query;
        var scalar = Command.ExecuteScalar();
        scalar.Should().Be(42);
    }
    
    [Theory]
    [InlineData("SELECT ?1;")]
    [InlineData("SELECT ?;")]
    [InlineData("SELECT $1;")]
    public void BindSingleValueNullTest(string query)
    {
		Command.Parameters.Add(new DuckDBParameter("1", null));
		Command.CommandText = query;
        var scalar = Command.ExecuteScalar();
        scalar.Should().Be(DBNull.Value);
    }

    [Fact]
    public void BindNullValueTest()
    {

        Command.CommandText = "SELECT ?";
        Command.Parameters.Add(new DuckDBParameter());

        var scalar = Command.ExecuteScalar();
        scalar.Should().Be(DBNull.Value);
    }

    [Fact]
    public void ParameterCountMismatchTest()
    {
		Command.CommandText = "SELECT ?";
		Command.Invoking(dbCommand => dbCommand.ExecuteScalar()).Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void PrepareCommandNoOperationTest()
    {
        Command.CommandText = "SELECT ? FROM nowhere";
		Command.Invoking(dbCommand => dbCommand.Prepare()).Should().NotThrow();
    }

    [Fact]
    public void ParameterConstructorTests()
    {
        Command.CommandText = "CREATE TABLE ParameterConstructorTests (key INTEGER, value double, State Boolean, ErrorCode Long, value2 float)";
		Command.ExecuteNonQuery();

		Command.CommandText = "Insert Into ParameterConstructorTests values (?,?,?,?,?)";
		Command.Parameters.Add(new DuckDBParameter(DbType.Double, 2.4));
		Command.Parameters.Add(new DuckDBParameter(true));
        Command.Parameters.Insert(0, new DuckDBParameter(2));
		Command.Parameters.RemoveAt(2);
		Command.Parameters.AddRange(new List<DuckDBParameter>{new()
        {
            Value = true
        }, new(24), new(2.0f)});

		Command.ExecuteNonQuery();

		Command.CommandText = "select * from ParameterConstructorTests";
		Command.Parameters.Clear();

        var dataReader = Command.ExecuteReader();
        dataReader.Read();

        dataReader.GetInt32(0).Should().Be(2);
        dataReader.GetDouble(1).Should().Be(2.4);
        dataReader.GetBoolean(2).Should().Be(true);
        dataReader.GetInt32(3).Should().Be(24);
        dataReader.GetFloat(4).Should().Be(2.0f);
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
        using var defer = new Defer(() => Connection.Execute("DROP TABLE ParametersTestKeyValue;"));

        Command.CommandText = "CREATE TABLE ParametersTestKeyValue (KEY INTEGER, VALUE TEXT)";
		Command.ExecuteNonQuery();
		Command.CommandText = "INSERT INTO ParametersTestKeyValue (KEY, VALUE) VALUES (42, 'test string');";
		Command.ExecuteNonQuery();

		Command.CommandText = queryStatement;
		Command.Parameters.Add(new DuckDBParameter("1", 42));
		Command.Parameters.Add(new DuckDBParameter("2", "hello"));
        var affectedRows = Command.ExecuteNonQuery();
        affectedRows.Should().NotBe(0);
    }

    [Theory]
    [InlineData("INSERT INTO ParametersTestKeyValue (KEY, VALUE) VALUES ($key, $value)")]
    [InlineData("UPDATE ParametersTestKeyValue SET KEY = $key, VALUE = $value;")]
    public void BindMultipleValuesTestNamedParameters(string queryStatement)
    {
        using var defer = new Defer(() => Connection.Execute("DROP TABLE ParametersTestKeyValue;"));

		Command.CommandText = "CREATE TABLE ParametersTestKeyValue (KEY INTEGER, VALUE TEXT)";
		Command.ExecuteNonQuery();
		Command.CommandText = "INSERT INTO ParametersTestKeyValue (KEY, VALUE) VALUES (42, 'test string');";
		Command.ExecuteNonQuery();

		Command.CommandText = queryStatement;
		Command.Parameters.Add(new DuckDBParameter("key", 42));
		Command.Parameters.Add(new DuckDBParameter("value", "hello"));
        var affectedRows = Command.ExecuteNonQuery();
        affectedRows.Should().NotBe(0);
    }

    [Theory]
    [InlineData("INSERT INTO ParametersTestInvalidOrderKeyValue (KEY, VALUE) VALUES (?2, ?1)")]
    [InlineData("INSERT INTO ParametersTestInvalidOrderKeyValue (KEY, VALUE) VALUES ($2, $1)")]
    [InlineData("UPDATE ParametersTestInvalidOrderKeyValue SET Key = ?2, Value = ?1;")]
    [InlineData("UPDATE ParametersTestInvalidOrderKeyValue SET Key = $2, Value = $1;")]
    public void BindMultipleValuesInvalidOrderTest(string queryStatement)
    {
        using var defer = new Defer(() => Connection.Execute("DROP TABLE ParametersTestInvalidOrderKeyValue;"));

		Command.CommandText = "CREATE TABLE ParametersTestInvalidOrderKeyValue (KEY INTEGER, VALUE TEXT)";
        Command.ExecuteNonQuery();
        Command.CommandText = "INSERT INTO ParametersTestInvalidOrderKeyValue (KEY, VALUE) VALUES (42, 'test string');";
        Command.ExecuteNonQuery();

        Command.CommandText = queryStatement;
        Command.Parameters.Add(new DuckDBParameter("param1", 42));
        Command.Parameters.Add(new DuckDBParameter("param2", "hello"));
        Command.Invoking(cmd => cmd.ExecuteNonQuery())
            .Should().ThrowExactly<DuckDBException>();

        Command.Parameters.Clear();
        Command.Parameters.Add(new DuckDBParameter(42));
        Command.Parameters.Add(new DuckDBParameter("hello"));
        Command.Invoking(cmd => cmd.ExecuteNonQuery())
            .Should().ThrowExactly<DuckDBException>();
    }

    [Theory]
    // Dapper supports ? placeholders when using both DynamicParameters and an object
    [InlineData("INSERT INTO DapperParametersObjectBindingTest VALUES (?, ?);")]
    [InlineData("UPDATE DapperParametersObjectBindingTest SET a = ?, b = ?;")]
    public void BindDapperWithObjectTest(string queryStatement)
    {
        using var defer = new Defer(() => Connection.Execute("DROP TABLE DapperParametersObjectBindingTest;"));

        Connection.Execute("CREATE TABLE DapperParametersObjectBindingTest (a INTEGER, b TEXT);");
        Connection.Execute("INSERT INTO DapperParametersObjectBindingTest (a, b) VALUES (42, 'test string');");

        var dp = new DynamicParameters();
        dp.Add("?1", 1);
        dp.Add("?2", "test");

        Connection.Execute(queryStatement, dp).Should().BeGreaterOrEqualTo(1);
    }

    [Theory]
    // Dapper supports ? placeholders when using both DynamicParameters and an object
    [InlineData("INSERT INTO DapperParametersObjectBindingTest VALUES ($foo, $bar);")]
    [InlineData("UPDATE DapperParametersObjectBindingTest SET a = $foo, b = $bar;")]
    public void BindDapperWithObjectTestNamesParameters(string queryStatement)
    {
        using var defer = new Defer(() => Connection.Execute("DROP TABLE DapperParametersObjectBindingTest;"));

        Connection.Execute("CREATE TABLE DapperParametersObjectBindingTest (a INTEGER, b TEXT);");
        Connection.Execute("INSERT INTO DapperParametersObjectBindingTest (a, b) VALUES (42, 'test string');");

        var dp = new DynamicParameters();
        dp.Add("foo", 1);
        dp.Add("bar", "test");
        
        Connection.Execute(queryStatement, dp).Should().BeGreaterOrEqualTo(1);
        Connection.Execute(queryStatement, new { foo = 1, bar = "test" }).Should().BeGreaterOrEqualTo(1, "Passing parameters as object should work");
    }

    [Theory]
    // Dapper supports such placeholders when using DynamicParameters
    [InlineData("INSERT INTO DapperParametersDynamicParamsBindingTest VALUES (?1, ?2);")]
    [InlineData("INSERT INTO DapperParametersDynamicParamsBindingTest VALUES ($1, $2);")]
    [InlineData("UPDATE DapperParametersDynamicParamsBindingTest SET a = ?1, b = ?2;")]
    [InlineData("UPDATE DapperParametersDynamicParamsBindingTest SET a = $1, b = $2;")]
    public void BindDapperDynamicParamsOnlyTest(string queryStatement)
    {
        using var defer = new Defer(() => Connection.Execute("DROP TABLE DapperParametersDynamicParamsBindingTest;"));

        Connection.Execute("CREATE TABLE DapperParametersDynamicParamsBindingTest (a INTEGER, b TEXT);");

        var dp = new DynamicParameters();
        dp.Add("1", 1);
        dp.Add("2", "test");

        Connection.Execute(queryStatement, dp).Should().BeLessOrEqualTo(1);
    }
    
    [Theory]
    [InlineData("SELECT ?1;")]
    [InlineData("SELECT ?;")]
    [InlineData("SELECT $1;")]
    public void BindSingleValueDapperNullTest(string query)
    {
        var parameters = new DynamicParameters();
        parameters.Add("1", null);
        var scalar = Connection.QuerySingle<long?>(query, parameters);
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
        using var defer = new Defer(() => Connection.Execute("DROP TABLE DapperParametersObjectBindingFailTest;"));

        Connection.Execute("CREATE TABLE DapperParametersObjectBindingFailTest (a INTEGER, b TEXT);");

        Connection.Invoking(con => con.Execute(queryStatement, new { param1 = 1, param2 = "hello" }))
            .Should().ThrowExactly<InvalidOperationException>();
    }

	[Fact]
	public void BindUnreferencedNamedParameterInParameterlessQueryTest()
	{
		Command.CommandText = "SELECT 42";
		Command.Parameters.Add(new DuckDBParameter("unused", 24));
		var scalar = Command.ExecuteScalar();
		scalar.Should().Be(42);
	}


	[Fact]
    public void BindUnreferencedNamedParameterTest()
    {
		Command.CommandText = "SELECT $used";
		Command.Parameters.Add(new DuckDBParameter("unused", 24));
		Command.Parameters.Add(new DuckDBParameter("used", 42));
		var scalar = Command.ExecuteScalar();
        scalar.Should().Be(42);
    }

    [Fact]
    public void BindUnreferencedPositionalParameterTest()
    {
		Command.CommandText = "SELECT 1";
		Command.Parameters.Add(new DuckDBParameter(42));    // unused
        var scalar = Command.ExecuteScalar();
        scalar.Should().Be(1);
    }
}
