using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DuckDB.NET.Data.Arrow;
using DuckDB.NET.Data.Execution;
using DuckDB.NET.Data.PreparedStatement;

namespace DuckDB.NET.Data;

public class DuckDBCommand : DbCommand
{
    private DuckDBConnection? connection;
    private readonly DuckDBParameterCollection parameters = new();

    // Set by Prepare(). The entry belongs to the connection, which destroys it when it closes or when the text is
    // prepared again; the command then takes the connection's current one on its next execution. After a failed
    // Prepare() the command has no entry and prepares again on its next execution.
    private bool prepared;
    private bool preparePending;
    private PreparedStatementEntry? preparedEntry;

    protected override DbTransaction? DbTransaction { get; set; }
    protected override DbParameterCollection DbParameterCollection => parameters;

    public new virtual DuckDBParameterCollection Parameters => parameters;

    public override int CommandTimeout { get; set; }
    public override CommandType CommandType { get; set; } = CommandType.Text;
    public override bool DesignTimeVisible { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }

    /// <summary>
    /// Whether results stream, which is what DuckDB's C API V2 does. Defaults to true.
    /// </summary>
    /// <remarks>
    /// A streaming result is produced while it is read: the statement runs as the reader advances, and an error can
    /// be reported by <see cref="DbDataReader.Read"/>. A connection has one open result at a time, so another command
    /// on the connection fails until the reader is read to its end or closed. Materialized results (false) are not
    /// supported on this branch yet (docs/V2-MIGRATION.md).
    /// </remarks>
    public bool UseStreamingMode { get; set; } = true;

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

    public override void Cancel() => connection?.Interrupt();

    public override int ExecuteNonQuery()
    {
        EnsureConnectionOpen();

        var results = ExecuteStatements();

        // DbCommand.ExecuteNonQuery: "For UPDATE, INSERT, and DELETE statements, the return value is the number of
        // rows affected by the command. For all other types of statements, the return value is -1."
        var count = -1;

        // Each statement runs to its end: V2 runs a statement while its result is read.
        foreach (var result in results)
        {
            using (result)
            {
                var rows = result.Finish();

                if (CountsRows(result.StatementType))
                {
                    count = Math.Max(count, 0) + (int)rows;
                }
            }
        }

        return count;
    }

    /// <summary>The statements whose affected-row count ADO.NET reports; every other statement reports -1.</summary>
    internal static bool CountsRows(DuckDBStatementType statementType) =>
        statementType is DuckDBStatementType.Insert or DuckDBStatementType.Update or DuckDBStatementType.Delete or DuckDBStatementType.Copy or DuckDBStatementType.MergeInto;

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

        throw new NotSupportedException("Arrow streams have not been migrated to DuckDB's C API V2 yet (docs/V2-MIGRATION.md, stage 7).");
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
    /// (a column altered to another type, CREATE OR REPLACE or DROP of a view or macro, SET), call Prepare() again.
    /// As IDbCommand.Prepare documents, throws InvalidOperationException when "The Connection is not set. -or- The
    /// Connection is not Open()."
    /// </remarks>
    public override void Prepare()
    {
        EnsureConnectionOpen();

        prepared = true;
        try
        {
            preparedEntry = connection!.PrepareStatement(CommandText);
            preparePending = false;
        }
        catch
        {
            // The command keeps no statement that no longer matches the database (a view it reads was dropped), but
            // stays prepared: its next execution prepares again, reporting the error if it remains and keeping the
            // statement if the cause has passed (another result was open on the connection).
            preparedEntry = null;
            preparePending = true;
            throw;
        }
    }

    private void Unprepare()
    {
        prepared = false;
        preparePending = false;
        preparedEntry = null;
    }

    private IEnumerable<DuckDBV2StatementResult> ExecuteStatements()
    {
        if (!UseStreamingMode)
        {
            throw new NotSupportedException("Materialized results (UseStreamingMode = false) are not supported yet: results stream " +
                                            "with DuckDB's C API V2 (docs/V2-MIGRATION.md).");
        }

        if (prepared || parameters.Count > 0)
        {
            throw new NotSupportedException("Parameters and prepared commands have not been migrated to DuckDB's C API V2 yet " +
                                            "(docs/V2-MIGRATION.md, stage 3).");
        }

        return DuckDBV2Statements.Execute(connection!.V2Connection, CommandText);
    }

    // An iterator, like PrepareMultiple, so that an error (also from preparing again) is raised where the reader
    // expects it: inside its constructor, which then closes the connection for CommandBehavior.CloseConnection.
    private IEnumerable<StatementResult> ExecutePrepared(DuckDBNativeConnection nativeConnection, DuckDBParameterCollection parameterCollection,
                                                         bool useStreamingMode)
    {
        if (preparePending || preparedEntry is { IsStale: true })
        {
            // The connection was closed since Prepare(), the text was prepared again, or Prepare() failed.
            preparedEntry = connection!.GetOrPrepareStatement(CommandText);
            preparePending = false;
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
