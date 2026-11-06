using System.Threading;

namespace DuckDB.NET.Data.PreparedStatement;

/// <summary>
/// A statement prepared once by <see cref="DuckDBCommand.Prepare"/> and kept by its connection, which destroys it when
/// it closes. What the binding needs (the count, the type of each parameter, the index of each name) is read from
/// DuckDB once, before any value is bound, instead of on every execution. Once a value is bound, DuckDB reports a
/// parameter whose type it could not infer (as in <c>SELECT $1</c>) as the type of that value until the bindings are
/// cleared, so the types must not be read in between.
/// </summary>
internal sealed class PreparedStatementEntry : IDisposable
{
    private readonly DuckDBPreparedStatement statement;
    private readonly DuckDBLogicalType[] parameterLogicalTypes;
    private readonly DuckDBType[] parameterTypes;
    private Dictionary<string, long>? parameterIndexes;
    private int inUse;
    private volatile bool retired;

    public PreparedStatementEntry(DuckDBPreparedStatement statement)
    {
        this.statement = statement;
        StatementType = NativeMethods.PreparedStatements.DuckDBPreparedStatementType(statement);
        ParameterCount = NativeMethods.PreparedStatements.DuckDBParams(statement);

        parameterLogicalTypes = new DuckDBLogicalType[ParameterCount];
        parameterTypes = new DuckDBType[ParameterCount];

        try
        {
            for (var index = 0; index < ParameterCount; index++)
            {
                parameterLogicalTypes[index] = NativeMethods.PreparedStatements.DuckDBParamLogicalType(statement, index + 1);
                parameterTypes[index] = NativeMethods.LogicalType.DuckDBGetTypeId(parameterLogicalTypes[index]);
            }
        }
        catch
        {
            DisposeParameterTypes();
            throw;
        }
    }

    public DuckDBStatementType StatementType { get; }

    public long ParameterCount { get; }

    public bool IsDisposed { get; private set; }

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
    /// on two threads of one connection): the caller then prepares a statement of its own, as an unprepared command
    /// does. DuckDB does not allow binding one statement from two threads.
    /// </summary>
    public bool TryRent() => !IsDisposed && Interlocked.CompareExchange(ref inUse, 1, 0) == 0;

    public void Return() => Volatile.Write(ref inUse, 0);

    /// <summary>
    /// Binds the parameters and executes. The bindings of the previous execution are cleared first, so a named
    /// parameter that is no longer in the collection is reported as missing instead of keeping its old value.
    /// </summary>
    public DuckDBResult Execute(DuckDBParameterCollection parameters, bool useStreamingMode, DuckDBNativeConnection connection)
    {
        if (!NativeMethods.PreparedStatements.DuckDBClearBindings(statement).IsSuccess())
        {
            var errorMessage = NativeMethods.PreparedStatements.DuckDBPrepareError(statement);
            throw new InvalidOperationException($"Unable to clear the bindings of the prepared statement: {errorMessage}");
        }

        PreparedStatement.BindParameters(statement, parameters, this);
        ExecutionCount++;

        return PreparedStatement.Execute(statement, useStreamingMode, connection);
    }

    public (DuckDBLogicalType LogicalType, DuckDBType Type) GetParameterType(long index) =>
        (parameterLogicalTypes[index - 1], parameterTypes[index - 1]);

    public bool TryGetParameterIndex(string name, out long index)
    {
        parameterIndexes ??= new Dictionary<string, long>(StringComparer.Ordinal);

        if (!parameterIndexes.TryGetValue(name, out index))
        {
            // A name the statement does not declare is kept as 0, so it is not looked up again.
            index = NativeMethods.PreparedStatements.DuckDBBindParameterIndex(statement, out var found, name).IsSuccess() ? found : 0;
            parameterIndexes.Add(name, index);
        }

        return index > 0;
    }

    public void Dispose()
    {
        if (IsDisposed)
        {
            return;
        }

        IsDisposed = true;
        DisposeParameterTypes();
        statement.Dispose();
    }

    private void DisposeParameterTypes()
    {
        foreach (var logicalType in parameterLogicalTypes)
        {
            logicalType?.Dispose();
        }
    }
}
