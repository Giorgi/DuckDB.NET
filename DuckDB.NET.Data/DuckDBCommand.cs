using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DuckDB.NET.Data.Arrow;

namespace DuckDB.NET.Data;

public class DuckDBCommand : DbCommand
{
    private DuckDBConnection? connection;
    private readonly DuckDBParameterCollection parameters = new();
    private bool readerOpen;

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
            if (readerOpen)
                throw new InvalidOperationException("CommandText cannot be changed while a reader is open.");
            field = value ?? string.Empty;
        }
    } = string.Empty;

    protected override DbConnection? DbConnection
    {
        get => connection;
        set
        {
            if (readerOpen)
                throw new InvalidOperationException("Connection cannot be changed while a reader is open.");
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

    public override void Cancel()
    {
        if (connection?.State == ConnectionState.Open)
            connection.NativeConnection.Interrupt();
    }

    public override int ExecuteNonQuery()
    {
        EnsureConnectionOpen();

        var results = PreparedStatement.PreparedStatement.PrepareMultiple(connection!.NativeConnection, CommandText, parameters, UseStreamingMode);

        var count = 0;
        var returnedRows = false;

        foreach (var result in results)
        {
            var current = result;
            if (NativeMethods.Query.DuckDBResultReturnType(current) == DuckDBResultType.QueryResult)
                returnedRows = true;
            else
                count += (int)NativeMethods.Query.DuckDBRowsChanged(ref current);
            result.Close();
        }

        return returnedRows ? -1 : count;
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
        if (readerOpen)
            throw new InvalidOperationException("A reader is already open for this command.");

        var results = PreparedStatement.PreparedStatement.PrepareMultiple(connection!.NativeConnection, CommandText, parameters, UseStreamingMode);
        readerOpen = true;
        try
        {
            return new DuckDBDataReader(this, results, behavior);
        }
        catch
        {
            readerOpen = false;
            throw;
        }
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

        var results = PreparedStatement.PreparedStatement.PrepareMultiple(connection!.NativeConnection, CommandText, parameters, UseStreamingMode);

        foreach (var result in results)
        {
            var current = result;

            if (NativeMethods.Query.DuckDBResultReturnType(current) == DuckDBResultType.QueryResult)
            {
                return new DuckDBArrowArrayStream(current, connection.NativeConnection);
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
        EnsureConnectionOpen();
        if (string.IsNullOrWhiteSpace(CommandText))
            throw new InvalidOperationException("CommandText is required.");
    }

    protected override DbParameter CreateDbParameter() => new DuckDBParameter();

    internal void CloseConnection() => Connection!.Close();

    internal void ReaderClosed() => readerOpen = false;

    private void EnsureConnectionOpen([CallerMemberName] string operation = "")
    {
        if (Connection is null || Connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException($"{operation} requires an open connection");
        }
        if (string.IsNullOrWhiteSpace(CommandText))
            throw new InvalidOperationException("CommandText is required.");
        var openConnection = connection!;
        if (DbTransaction is not null && !ReferenceEquals(DbTransaction.Connection, openConnection))
            throw new InvalidOperationException("The command transaction belongs to a different connection.");
        if (openConnection.Transaction is not null && !ReferenceEquals(DbTransaction, openConnection.Transaction))
            throw new InvalidOperationException("The command transaction does not match the active connection transaction.");
    }
}
