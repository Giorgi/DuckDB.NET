using System.Threading;
using DuckDB.NET.Data.Execution;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.PreparedStatement;

/// <summary>
/// A statement prepared once by <see cref="DuckDBCommand.Prepare"/> and kept by its connection, which destroys it when
/// it closes. What the binding needs (each parameter's name, which is its binding key, and its type) is read once,
/// when it is prepared. V2 binds the values per execution and keeps nothing between executions, so unlike V1 there
/// are no bindings to clear and an untyped parameter's type cannot drift.
/// </summary>
internal sealed unsafe class PreparedStatementEntry : IDisposable
{
    private readonly DuckDBV2PreparedStatement statement;
    private readonly DuckDBV2Parameters? parameters;
    private int inUse;
    private int disposed;
    private volatile bool retired;

    public PreparedStatementEntry(DuckDBV2PreparedStatement statement, DuckDBStatementType statementType, DuckDBV2Parameters? parameters)
    {
        this.statement = statement;
        this.parameters = parameters;
        StatementType = statementType;
    }

    public DuckDBStatementType StatementType { get; }

    public long ParameterCount => parameters?.Count ?? 0;

    public bool IsDisposed => Volatile.Read(ref disposed) != 0;

    /// <summary>
    /// Destroyed, or replaced by a newer preparation of the same text: a command that holds it takes the current
    /// statement from the connection instead.
    /// </summary>
    public bool IsStale => IsDisposed || retired;

    /// <summary>How many times it was executed; tests use it to see which statement an execution took.</summary>
    public int ExecutionCount { get; private set; }

    public void MarkRetired() => retired = true;

    /// <summary>
    /// Takes the statement for one execution. False when another execution has it (two commands with the same text,
    /// on two threads of one connection): the caller then runs the text unprepared, as an unprepared command does.
    /// </summary>
    public bool TryRent() => !IsDisposed && Interlocked.CompareExchange(ref inUse, 1, 0) == 0;

    /// <summary>
    /// Gives the statement back after an execution. One that was replaced while it was executing is destroyed here:
    /// nobody else uses it any more, and the connection would otherwise keep it until it closes.
    /// </summary>
    public void Return()
    {
        // A full fence: the read of retired below must not move before this write, or a Retire() on another thread
        // could see the entry still taken while this thread sees it not yet retired, and nobody would destroy it.
        Interlocked.Exchange(ref inUse, 0);

        if (retired && TryRent())
        {
            Dispose();
        }
    }

    /// <summary>Binds the collection's values for this execution and executes; the result streams.</summary>
    public DuckDBV2StatementResult Execute(DuckDBParameterCollection parameterCollection, DuckDBV2Connection connection)
    {
        using var lease = statement.Acquire();
        var pointer = lease.Pointer;

        // Executing a prepared statement binds it again, and calls the callbacks of C# functions.
        using var scope = DuckDBV2CallbackScope.EnterExecution(connection);

        _duckdb_v2_error_info* error = null;
        _duckdb_v2_result* result;

        var code = parameters is null
            ? NativeMethodsV2.duckdb_v2_prepared_statement_execute(pointer, null, null, null, 0, &result, &error)
            : parameters.Execute(connection, parameterCollection,
                                 (names, lengths, values, count, output, executeError) =>
                                     NativeMethodsV2.duckdb_v2_prepared_statement_execute(pointer, names, lengths, values, count, (_duckdb_v2_result**)output, executeError),
                                 (void**)&result, &error);

        ExecutionCount++;

        return DuckDBV2Statements.Wrap(code, result, ref error, StatementType, connection);
    }

    /// <summary><see cref="Execute"/> for Arrow: the result produces Arrow arrays, in batches of V2's default size.</summary>
    public DuckDBV2ArrowResult ExecuteArrow(DuckDBParameterCollection parameterCollection, DuckDBV2Connection connection)
    {
        using var lease = statement.Acquire();
        var pointer = lease.Pointer;

        using var scope = DuckDBV2CallbackScope.EnterExecution(connection);

        _duckdb_v2_error_info* error = null;
        _duckdb_v2_arrow_result* result;

        var code = parameters is null
            ? NativeMethodsV2.duckdb_v2_prepared_statement_execute_arrow(pointer, null, null, null, 0, 0, &result, &error)
            : parameters.Execute(connection, parameterCollection,
                                 (names, lengths, values, count, output, executeError) =>
                                     NativeMethodsV2.duckdb_v2_prepared_statement_execute_arrow(pointer, names, lengths, values, count, 0, (_duckdb_v2_arrow_result**)output, executeError),
                                 (void**)&result, &error);

        ExecutionCount++;

        return DuckDBV2Statements.WrapArrow(code, result, ref error, connection);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        parameters?.Dispose();
        statement.Dispose();
    }
}
