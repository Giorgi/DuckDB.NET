using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DuckDB.NET.Data.Arrow;

namespace DuckDB.NET.Data;

public class DuckDBCommand : DbCommand
{
    private DuckDBConnection? connection;
    private readonly DuckDBParameterCollection parameters = new();

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
            // TODO: We shouldn't be able to change the CommandText when the command is in execution (requires CommandState implementation)
            field = value ?? string.Empty;
        }
    } = string.Empty;

    protected override DbConnection? DbConnection
    {
        get => connection;
        set => connection = (DuckDBConnection?)value;
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

        var results = PreparedStatement.PreparedStatement.PrepareMultiple(connection!.NativeConnection, CommandText, parameters, UseStreamingMode);

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

        var results = PreparedStatement.PreparedStatement.PrepareMultiple(connection!.NativeConnection, CommandText, parameters, UseStreamingMode);

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

        var results = PreparedStatement.PreparedStatement.PrepareMultiple(connection!.NativeConnection, CommandText, parameters, UseStreamingMode);

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

    public override void Prepare() { }

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