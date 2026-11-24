namespace DuckDB.NET.Test.V2;

// What changed for scalar functions with the C API V2: overloads, and how a C# exception finds its command now that V2
// has no connection id (docs/V2-MIGRATION.md, stage 5).
public class V2ScalarFunctionTests
{
    [Fact]
    public void TwoFunctionsWithTheSameNameAreOverloads()
    {
        using var connection = Open("DataSource=:memory:");
        connection.RegisterScalarFunction<int, string>("describe_value", (int x) => $"int {x}");
        connection.RegisterScalarFunction<string, string>("describe_value", (string x) => $"string {x}");

        Scalar(connection, "SELECT describe_value(42)").Should().Be("int 42");
        Scalar(connection, "SELECT describe_value('a')").Should().Be("string a");
    }

    // Registered again with the same signature, a function replaces the one before (ALTER_ON_CONFLICT), with no error.
    [Fact]
    public void AFunctionRegisteredAgainWithTheSameSignatureReplacesTheFirst()
    {
        using var connection = Open("DataSource=:memory:");
        connection.RegisterScalarFunction<int, int>("twice", (int x) => x * 2);
        connection.RegisterScalarFunction<int, int>("twice", (int x) => x * 3);

        Scalar(connection, "SELECT twice(5)").Should().Be(15);
    }

    [Fact]
    public void TheExceptionReachesTheConnectionThatRanTheFunction()
    {
        // The shared in-memory database is the whole test run's, so the function gets a name of its own.
        using var first = Open("DataSource=:memory:?cache=shared");
        using var second = Open("DataSource=:memory:?cache=shared");
        var function = $"fails_on_negative_{Guid.NewGuid():N}";

        var thrown = new InvalidOperationException("from the function");
        first.RegisterScalarFunction<int, int>(function, (int x) => x < 0 ? throw thrown : x);

        // Registered through the first connection, the function is in the database's catalog and the second runs it.
        var error = second.Invoking(c => Scalar(c, $"SELECT {function}(-1)")).Should().Throw<DuckDBException>().Which;
        error.InnerException.Should().BeSameAs(thrown);

        // The first connection ran nothing that failed, so its next error has no inner exception.
        var unrelated = first.Invoking(c => Scalar(c, "SELECT 'x'::INTEGER")).Should().Throw<DuckDBException>().Which;
        unrelated.InnerException.Should().BeNull();
    }

    [Fact]
    public void AnErrorThatDuckDBSwallowsLeavesNoExceptionForTheNextOne()
    {
        using var connection = Open("DataSource=:memory:");
        connection.RegisterScalarFunction<int, int>("always_fails", (int x) => throw new InvalidOperationException("swallowed"));

        // TRY turns an error into NULL, so the function's exception is never reported.
        Scalar(connection, "SELECT TRY(always_fails(1))").Should().Be(DBNull.Value);

        var unrelated = connection.Invoking(c => Scalar(c, "SELECT 'x'::INTEGER")).Should().Throw<DuckDBException>().Which;
        unrelated.InnerException.Should().BeNull();
    }

    [Fact]
    public void AnExceptionInAPreparedStatementIsAttachedOnEveryExecution()
    {
        using var connection = Open("DataSource=:memory:");
        connection.RegisterScalarFunction<int, int>("fails_on_negative", (int x) => x < 0 ? throw new InvalidOperationException($"refused {x}") : x);

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT fails_on_negative($1)";
        command.Parameters.Add(new DuckDBParameter(-1));
        command.Prepare();

        for (var i = 1; i <= 3; i++)
        {
            command.Parameters[0].Value = -i;
            command.Invoking(c => c.ExecuteScalar()).Should().Throw<DuckDBException>()
                   .WithInnerException<InvalidOperationException>().WithMessage($"refused {-i}");

            command.Parameters[0].Value = i;
            command.ExecuteScalar().Should().Be(i);
        }
    }

    private static DuckDBConnection Open(string connectionString)
    {
        var connection = new DuckDBConnection(connectionString);
        connection.Open();
        return connection;
    }

    private static object Scalar(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar();
    }
}
