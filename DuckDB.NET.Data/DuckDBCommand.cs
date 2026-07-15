using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DuckDB.NET.Data.Arrow;
using PreparedStatementBase = DuckDB.NET.Data.PreparedStatement.PreparedStatement;
using ReusablePreparedStatement = DuckDB.NET.Data.PreparedStatement.ReusablePreparedStatement;

namespace DuckDB.NET.Data;

public class DuckDBCommand : DbCommand
{
    private DuckDBConnection? connection;
    private readonly DuckDBParameterCollection parameters = new();
    private ReusablePreparedStatement? preparedStatement;
    private DuckDBConnection? preparedConnection;
    private List<(DuckDBConnection Connection, ReusablePreparedStatement Statement)>? deferredPreparedStatements;
    private HashSet<DuckDBConnection>? registeredConnections;
    private int activeExecutions;
    private bool disposed;

    protected override DbTransaction? DbTransaction { get; set; }
    protected override DbParameterCollection DbParameterCollection => parameters;

    public new virtual DuckDBParameterCollection Parameters => parameters;

    public override int CommandTimeout { get; set; }
    public override CommandType CommandType { get; set; }
    public override bool DesignTimeVisible { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }

    /// <summary>
    /// A flag to determine whether to use streaming mode or not when executing a query. Defaults to false.
    /// In streaming mode DuckDB will use less RAM but query execution might be slower. Applies only to queries that return a result-set.
    /// </summary>
    /// <remarks>
    /// Streaming mode uses `duckdb_execute_prepared_streaming` and `duckdb_stream_fetch_chunk`, non-streaming (materialized) mode uses `duckdb_execute_prepared` and `duckdb_result_get_chunk`.
    /// </remarks>
    public bool UseStreamingMode { get; set; } = false;

    [AllowNull]
    [DefaultValue("")]
    public override string CommandText
    {
        get;
        set
        {
            EnsureNotDisposed();

            var newValue = value ?? string.Empty;
            if (string.Equals(field, newValue, StringComparison.Ordinal))
            {
                return;
            }

            InvalidatePreparedStatements();
            field = newValue;
        }
    } = string.Empty;

    protected override DbConnection? DbConnection
    {
        get => connection;
        set
        {
            EnsureNotDisposed();

            var newConnection = (DuckDBConnection?)value;
            if (ReferenceEquals(connection, newConnection))
            {
                return;
            }

            InvalidatePreparedStatements();
            connection = newConnection;
        }
    }

    public DuckDBCommand()
    { }

    public DuckDBCommand(string commandText)
    {
        CommandText = commandText;
    }

    public DuckDBCommand(string commandText, DuckDBConnection connection)
        : this(commandText)
    {
        Connection = connection;
    }

    public override void Cancel() => connection?.NativeConnection.Interrupt();

    public override int ExecuteNonQuery()
    {
        EnsureConnectionOpen();

        var results = ExecuteStatements();

        var count = 0;

        foreach (var result in results)
        {
            var current = result;
            count += (int)NativeMethods.Query.DuckDBRowsChanged(ref current);
            result.Close();
        }

        return count;
    }

    public override object? ExecuteScalar()
    {
        EnsureConnectionOpen();

        using var reader = ExecuteReader();
        return reader.Read() ? reader.GetValue(0) : null;
    }

    public new DuckDBDataReader ExecuteReader()
    {
        return (DuckDBDataReader)base.ExecuteReader();
    }

    public new DuckDBDataReader ExecuteReader(CommandBehavior behavior)
    {
        return (DuckDBDataReader)base.ExecuteReader(behavior);
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        EnsureConnectionOpen();

        var results = ExecuteStatements();

        var reader = new DuckDBDataReader(this, results, behavior);

        return reader;
    }

    /// <summary>
    /// Executes the command and returns the first result set as an Apache Arrow
    /// <see cref="IArrowArrayStream"/>. Each DuckDB data chunk is converted to an Arrow record batch
    /// using DuckDB's Arrow C Data Interface, with no row-by-row marshaling.
    /// When <see cref="UseStreamingMode"/> is enabled, record batches are produced lazily from a
    /// streaming result (bounded memory), otherwise the result is materialized first.
    /// The caller owns the returned stream and must dispose it.
    /// </summary>
    public IArrowArrayStream ExecuteArrowStream()
    {
        EnsureConnectionOpen();

        var results = ExecuteStatements();

        foreach (var result in results)
        {
            var current = result;

            if (NativeMethods.Query.DuckDBResultReturnType(current) == DuckDBResultType.QueryResult)
            {
                return new DuckDBArrowArrayStream(current);
            }

            current.Close();
        }

        throw new InvalidOperationException("The command did not return a result set.");
    }

    /// <summary>
    /// Executes the command and asynchronously streams the first result set as Apache Arrow
    /// <see cref="RecordBatch"/> values. The batches are produced lazily, one per DuckDB data chunk.
    /// Set <see cref="UseStreamingMode"/> to stream from a streaming result with bounded memory.
    /// </summary>
    public async IAsyncEnumerable<RecordBatch> ExecuteArrowBatchesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var stream = ExecuteArrowStream();

        try
        {
            while (await stream.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false) is { } batch)
            {
                yield return batch;
            }
        }
        finally
        {
            stream.Dispose();
        }
    }

    public override void Prepare()
    {
        EnsureNotDisposed();
        EnsureConnectionOpen();

        if (preparedStatement is not null)
        {
            return;
        }

        var statement = PreparedStatementBase.TryPrepareReusable(connection!.NativeConnection, CommandText);
        if (statement is null)
        {
            return;
        }

        preparedStatement = statement;
        preparedConnection = connection;
        RefreshPreparedCommandRegistrations();
    }

    protected override DbParameter CreateDbParameter() => new DuckDBParameter();

    internal void CloseConnection() => Connection!.Close();

    protected override void Dispose(bool disposing)
    {
        if (disposing && !disposed)
        {
            disposed = true;
            InvalidatePreparedStatements();

            if (activeExecutions == 0)
            {
                UnregisterFromConnections();
            }
        }

        base.Dispose(disposing);
    }

    private IEnumerable<DuckDBResult> ExecuteStatements()
    {
        EnsureNotDisposed();

        var nativeConnection = connection!.NativeConnection;
        var reusableStatement = preparedStatement;

        return reusableStatement is null
            ? PreparedStatementBase.PrepareMultiple(nativeConnection, CommandText, parameters, UseStreamingMode)
            : ExecutePreparedStatement(reusableStatement, nativeConnection);
    }

    private IEnumerable<DuckDBResult> ExecutePreparedStatement(
        ReusablePreparedStatement reusableStatement,
        DuckDBNativeConnection nativeConnection)
    {
        activeExecutions++;

        try
        {
            yield return reusableStatement.Execute(parameters, UseStreamingMode, nativeConnection);
        }
        finally
        {
            activeExecutions--;

            if (activeExecutions == 0)
            {
                DisposeDeferredPreparedStatements();
            }

            if (disposed && activeExecutions == 0)
            {
                UnregisterFromConnections();
            }
        }
    }

    private void InvalidatePreparedStatements()
    {
        var statement = preparedStatement;
        var statementConnection = preparedConnection;
        preparedStatement = null;
        preparedConnection = null;

        if (statement is null)
        {
            return;
        }

        if (activeExecutions > 0)
        {
            deferredPreparedStatements ??= [];
            deferredPreparedStatements.Add((statementConnection!, statement));
            RefreshPreparedCommandRegistrations();
            return;
        }

        statement.Dispose();
        RefreshPreparedCommandRegistrations();
    }

    private void DisposeDeferredPreparedStatements()
    {
        var deferredStatements = deferredPreparedStatements;
        if (deferredStatements is null)
        {
            return;
        }

        foreach (var deferredStatement in deferredStatements)
        {
            deferredStatement.Statement.Dispose();
        }

        deferredPreparedStatements = null;
        RefreshPreparedCommandRegistrations();
    }

    internal void OnConnectionClosing(DuckDBConnection closingConnection)
    {
        if (ReferenceEquals(preparedConnection, closingConnection))
        {
            preparedStatement?.Dispose();
            preparedStatement = null;
            preparedConnection = null;
        }

        for (var index = deferredPreparedStatements?.Count - 1 ?? -1; index >= 0; index--)
        {
            var deferredStatement = deferredPreparedStatements![index];
            if (!ReferenceEquals(deferredStatement.Connection, closingConnection))
            {
                continue;
            }

            deferredStatement.Statement.Dispose();
            deferredPreparedStatements.RemoveAt(index);
        }

        if (deferredPreparedStatements?.Count == 0)
        {
            deferredPreparedStatements = null;
        }

        RefreshPreparedCommandRegistrations();
    }

    private void RefreshPreparedCommandRegistrations()
    {
        List<DuckDBConnection>? connectionsToRemove = null;

        if (registeredConnections is not null)
        {
            foreach (var registeredConnection in registeredConnections)
            {
                if (IsConnectionRequired(registeredConnection))
                {
                    continue;
                }

                connectionsToRemove ??= [];
                connectionsToRemove.Add(registeredConnection);
            }
        }

        if (connectionsToRemove is not null)
        {
            foreach (var registeredConnection in connectionsToRemove)
            {
                registeredConnection.UnregisterPreparedCommand(this);
                registeredConnections!.Remove(registeredConnection);
            }
        }

        if (registeredConnections?.Count == 0)
        {
            registeredConnections = null;
        }

        if (preparedConnection is not null)
        {
            RegisterWithConnection(preparedConnection);
        }

        if (deferredPreparedStatements is not null)
        {
            foreach (var deferredStatement in deferredPreparedStatements)
            {
                RegisterWithConnection(deferredStatement.Connection);
            }
        }
    }

    private bool IsConnectionRequired(DuckDBConnection candidate)
    {
        if (ReferenceEquals(preparedConnection, candidate))
        {
            return true;
        }

        if (deferredPreparedStatements is null)
        {
            return false;
        }

        foreach (var deferredStatement in deferredPreparedStatements)
        {
            if (ReferenceEquals(deferredStatement.Connection, candidate))
            {
                return true;
            }
        }

        return false;
    }

    private void RegisterWithConnection(DuckDBConnection requiredConnection)
    {
        registeredConnections ??= [];
        if (registeredConnections.Add(requiredConnection))
        {
            requiredConnection.RegisterPreparedCommand(this);
        }
    }

    private void UnregisterFromConnections()
    {
        if (registeredConnections is null)
        {
            return;
        }

        foreach (var registeredConnection in registeredConnections)
        {
            registeredConnection.UnregisterPreparedCommand(this);
        }

        registeredConnections = null;
    }

    private void EnsureNotDisposed()
    {
        ObjectDisposedException.ThrowIf(disposed, this);
    }

    private void EnsureConnectionOpen([CallerMemberName] string operation = "")
    {
        EnsureNotDisposed();

        if (Connection is null || Connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException($"{operation} requires an open connection");
        }
    }
}
