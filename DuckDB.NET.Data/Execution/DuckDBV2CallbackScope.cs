using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Execution;

/// <summary>
/// The connection whose statement this thread is binding or running, for the bind callbacks of C# functions. DuckDB
/// binds a statement on the thread that calls it, inside <c>statement_execute</c>, <c>statement_bind</c>,
/// <c>prepared_statement_create</c> and <c>prepared_statement_execute</c>, and a statement it expands into several (a
/// dynamic PIVOT) when its result is fetched; <c>V2ScalarBindProbeTests</c> checks it. A bind callback reads the
/// connection here and hands it to the function's other callbacks as bind data, because they run on DuckDB's worker
/// threads and V2 gives a callback no way to find its connection.
/// </summary>
internal static class DuckDBV2CallbackScope
{
    [ThreadStatic] private static DuckDBV2Connection? current;

    public static DuckDBV2Connection? Current => current;

    /// <summary>Marks <paramref name="connection"/> as this thread's until the scope is disposed.</summary>
    public static Scope Enter(DuckDBV2Connection connection)
    {
        var previous = current;
        current = connection;
        return new Scope(previous);
    }

    /// <summary>
    /// <see cref="Enter"/> for a new execution: an exception left from an earlier one, whose error DuckDB never
    /// reported, must not be attached to this one's.
    /// </summary>
    public static Scope EnterExecution(DuckDBV2Connection connection)
    {
        connection.CallbackException = null;
        return Enter(connection);
    }

    public readonly ref struct Scope(DuckDBV2Connection? previous)
    {
        public void Dispose() => current = previous;
    }
}
