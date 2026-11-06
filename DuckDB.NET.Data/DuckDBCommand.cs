using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DuckDB.NET.Data.Arrow;
using DuckDB.NET.Data.PreparedStatement;

namespace DuckDB.NET.Data;

public class DuckDBCommand : DbCommand
{
    private DuckDBConnection? connection;
    private readonly DuckDBParameterCollection parameters = new();

    // Set by Prepare(). The entry belongs to the connection, which destroys it when it closes or when the text is
    // prepared again; the command then takes the connection's current one on its next execution.
    private bool prepared;
    private PreparedStatementEntry? preparedEntry;

    protected override DbTransaction? DbTransaction { get; set; }
    protected override DbParameterCollection DbParameterCollection => parameters;

    public new virtual DuckDBParameterCollection Parameters => parameters;

    public override int CommandTimeout { get; set; }
    public override CommandType CommandType { get; set; } = CommandType.Text;
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
            // TODO: We shouldn't be able to change the CommandText when the command is in execution (requires CommandState implementation)
            var commandText = value ?? string.Empty;
            if (!string.Equals(field, commandText, StringComparison.Ordinal))
            {
                Unprepare();
            }

            field = commandText;
        }
    } = string.Empty;

    protected override DbConnection? DbConnection
    {
        get => connection;
        set
        {
            if (!ReferenceEquals(connection, value))
            {
                Unprepare();
            }

            connection = (DuckDBConnection?)value;
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

        // DbCommand.ExecuteNonQuery: "For UPDATE, INSERT, and DELETE statements, the return value is the number of
        // rows affected by the command. For all other types of statements, the return value is -1."
        var count = -1;

        foreach (var (result, statementType) in results)
        {
            var current = result;
            if (CountsRows(statementType))
            {
                count = Math.Max(count, 0) + (int)GetRowsAffected(ref current);
            }

            result.Close();
        }

        return count;
    }

    /// <summary>The statements whose affected-row count ADO.NET reports; every other statement reports -1.</summary>
    internal static bool CountsRows(DuckDBStatementType statementType) =>
        statementType is DuckDBStatementType.Insert or DuckDBStatementType.Update or DuckDBStatementType.Delete or DuckDBStatementType.Copy or DuckDBStatementType.MergeInto;

    internal static long GetRowsAffected(ref DuckDBResult result) =>
        NativeMethods.Query.DuckDBResultReturnType(result) == DuckDBResultType.QueryResult
            ? NativeMethods.Query.DuckDBRowCount(ref result)
            : NativeMethods.Query.DuckDBRowsChanged(ref result);

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

        foreach (var (result, _) in results)
        {
            var current = result;

            if (NativeMethods.Query.DuckDBResultReturnType(current) == DuckDBResultType.QueryResult)
            {
                return new DuckDBArrowArrayStream(current, connection!.NativeConnection);
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

    /// <summary>
    /// Prepares the statement of the command, so later executions only bind the parameters and execute: they skip
    /// splitting the text into statements, parsing and preparing. The connection keeps the statement until it closes,
    /// and commands with the same text on that connection share it. Each call prepares the statement anew, and the
    /// other commands with that text then use the new one. After the connection is closed and opened again, the next
    /// execution prepares it again. Changing <see cref="CommandText"/> or <see cref="Connection"/> undoes the
    /// preparation.
    /// </summary>
    /// <remarks>
    /// A command of several statements is not prepared: a later statement can depend on an earlier one having run,
    /// so each is still prepared just before it runs. Errors in the text are reported here instead of at execution.
    /// A prepared statement keeps what was true when it was prepared: the types of its parameters (values are
    /// converted to them), and the definitions of the views and macros and the settings it uses. After changing them
    /// (a column altered to another type, CREATE OR REPLACE VIEW, SET), call Prepare() again.
    /// As IDbCommand.Prepare documents, throws InvalidOperationException when "The Connection is not set. -or- The
    /// Connection is not Open()."
    /// </remarks>
    public override void Prepare()
    {
        EnsureConnectionOpen();

        preparedEntry = connection!.PrepareStatement(CommandText);
        prepared = true;
    }

    private void Unprepare()
    {
        prepared = false;
        preparedEntry = null;
    }

    private IEnumerable<StatementResult> ExecuteStatements()
    {
        var nativeConnection = connection!.NativeConnection;

        return prepared
            ? ExecutePrepared(nativeConnection, parameters, UseStreamingMode)
            : PreparedStatement.PreparedStatement.PrepareMultiple(nativeConnection, CommandText, parameters, UseStreamingMode);
    }

    // An iterator, like PrepareMultiple, so that an error (also from preparing again) is raised where the reader
    // expects it: inside its constructor, which then closes the connection for CommandBehavior.CloseConnection.
    private IEnumerable<StatementResult> ExecutePrepared(DuckDBNativeConnection nativeConnection, DuckDBParameterCollection parameterCollection,
                                                         bool useStreamingMode)
    {
        if (preparedEntry is { IsStale: true })
        {
            // The connection was closed since Prepare(), or the text was prepared again.
            preparedEntry = connection!.GetOrPrepareStatement(CommandText);
        }

        var entry = preparedEntry;

        if (entry is null || !entry.TryRent())
        {
            // A command of several statements, or another execution is binding the shared statement (a command with
            // the same text on another thread).
            foreach (var result in PreparedStatement.PreparedStatement.PrepareMultiple(nativeConnection, CommandText, parameterCollection, useStreamingMode))
            {
                yield return result;
            }

            yield break;
        }

        DuckDBResult queryResult;
        try
        {
            queryResult = entry.Execute(parameterCollection, useStreamingMode, nativeConnection);
        }
        finally
        {
            // The result does not need the statement, so it is free again before the result is read.
            entry.Return();
        }

        yield return new StatementResult(queryResult, entry.StatementType);
    }

    protected override DbParameter CreateDbParameter() => new DuckDBParameter();

    internal void CloseConnection() => Connection!.Close();

    private void EnsureConnectionOpen([CallerMemberName] string operation = "")
    {
        if (Connection is null || Connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException($"{operation} requires an open connection");
        }
    }
}
