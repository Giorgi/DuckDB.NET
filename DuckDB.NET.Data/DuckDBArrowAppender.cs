using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using DuckDB.NET.Data.Execution;

namespace DuckDB.NET.Data;

/// <summary>
/// Appends Apache Arrow record batches to a DuckDB table, converted by DuckDB's Arrow importer with no row-by-row
/// marshaling and no copy of the data.
/// </summary>
/// <remarks>
/// Columns are matched by position: a batch must have as many columns as the table, and field names are
/// ignored. DuckDB converts each Arrow column and casts it to the table column type when the types differ,
/// so unlike <see cref="DuckDBAppender"/> this path is type-checked: a value that cannot be cast is an error.
/// <para>
/// Each batch is inserted by its own <c>INSERT</c>, when it is appended: its rows are in the table when
/// <see cref="AppendBatch"/> returns, and the batch can be disposed then. Instances are not thread-safe.
/// </para>
/// <para>
/// A batch that fails, because DuckDB cannot convert or cast it or because a row breaks a constraint such as NOT NULL,
/// PRIMARY KEY or UNIQUE, appends none of its rows and faults the appender: no more batches can be appended. The
/// batches appended before it stay in the table. Wrap the appends in a transaction if you need all-or-nothing.
/// </para>
/// <para>
/// The appender runs its inserts on its connection, so it cannot append while a data reader of the connection is open.
/// To copy a query result, read it on another connection.
/// </para>
/// </remarks>
public sealed class DuckDBArrowAppender : IDisposable
{
    private readonly DuckDBV2ArrowAppend.State state;
    private readonly DuckDBCommand insert;
    private readonly int columnCount;

    private bool closed;
    private bool isFaulted;

    internal DuckDBArrowAppender(DuckDBConnection connection, string? catalog, string? schema, string table)
    {
        var qualifiedTable = string.Join(".", new[] { catalog, schema, table }.Where(part => !string.IsNullOrEmpty(part))
                                                                                 .Select(DuckDBV2AppendBuffer.Quote));

        // Read now, so a missing table is reported when the appender is created.
        var (_, types) = DuckDBV2AppendBuffer.ReadColumns(connection.V2Connection, qualifiedTable);
        columnCount = types.Length;
        foreach (var type in types)
        {
            type.Dispose();
        }

        state = DuckDBV2ArrowAppend.CreateState(connection.V2Connection);

        try
        {
            insert = connection.CreateCommand();
            insert.CommandText = $"INSERT INTO {qualifiedTable} SELECT * FROM {DuckDBV2ArrowAppend.FunctionName}($1)";
            insert.Parameters.Add(new DuckDBParameter(state.Id));
            insert.Prepare();
        }
        catch
        {
            insert?.Dispose();
            state.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Appends every row of <paramref name="batch"/>.
    /// </summary>
    /// <exception cref="ArgumentException">The batch does not have as many columns as the table, or a column is not as
    /// long as the batch.</exception>
    /// <exception cref="DuckDBException">DuckDB cannot convert or cast the data, or a row breaks a constraint. The
    /// batch appends nothing and the appender is faulted.</exception>
    public void AppendBatch(RecordBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        EnsureUsable();

        AppendBatchCore(batch);
    }

    /// <summary>
    /// Reads <paramref name="stream"/> to the end and appends every batch, each by its own <c>INSERT</c>. Each batch read
    /// from the stream is disposed after it is appended; the stream itself is not disposed.
    /// </summary>
    /// <returns>The number of rows appended.</returns>
    public long AppendStream(IArrowArrayStream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);
        EnsureUsable();

        long rows = 0;

        while (ReadNextBatch(stream) is { } batch)
        {
            using (batch)
            {
                AppendBatchCore(batch);
                rows += batch.Length;
            }
        }

        return rows;
    }

    /// <summary>
    /// Reads <paramref name="stream"/> to the end and appends every batch, each by its own <c>INSERT</c>. Each batch read
    /// from the stream is disposed after it is appended; the stream itself is not disposed.
    /// </summary>
    /// <returns>The number of rows appended.</returns>
    public async ValueTask<long> AppendStreamAsync(IArrowArrayStream stream, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        EnsureUsable();

        long rows = 0;

        while (await stream.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false) is { } batch)
        {
            using (batch)
            {
                AppendBatchCore(batch);
                rows += batch.Length;
            }
        }

        return rows;
    }

    /// <summary>
    /// Does nothing: every batch is in the table once it is appended, so there is nothing left to drop.
    /// </summary>
    [Obsolete("Clear does nothing: each batch is inserted when it is appended, so there is nothing left to drop. " +
              "To undo appended batches, append them in a transaction and roll it back.")]
    public void Clear()
    {
        EnsureUsable();
    }

    public void Close()
    {
        if (closed)
        {
            throw new InvalidOperationException("Appender is already closed");
        }

        closed = true;

        try
        {
            insert.Dispose();
        }
        finally
        {
            state.Dispose();
        }
    }

    public void Dispose()
    {
        if (!closed)
        {
            Close();
        }
    }

    private void AppendBatchCore(RecordBatch batch)
    {
        if (batch.ColumnCount != columnCount)
        {
            throw new ArgumentException($"The batch has {batch.ColumnCount} columns but the table has {columnCount}. Columns are matched by position.", nameof(batch));
        }

        // RecordBatch does not check that its columns are as long as the batch. DuckDB's importer refuses such a batch as
        // well, but inside the INSERT, which faults the appender, and without naming the column. Checked here, the batch
        // is refused before anything runs and the appender stays usable.
        for (var index = 0; index < batch.ColumnCount; index++)
        {
            var length = batch.Column(index).Length;
            if (length != batch.Length)
            {
                throw new ArgumentException($"Column {index} has {length} rows but the batch has {batch.Length}.", nameof(batch));
            }
        }

        if (batch.Length == 0)
        {
            return;
        }

        var failed = true;
        state.Batch = batch;

        try
        {
            insert.ExecuteNonQuery();
            failed = false;
        }
        catch
        {
            isFaulted = true;
            throw;
        }
        finally
        {
            state.EndBatch(failed);
        }
    }

    private static RecordBatch? ReadNextBatch(IArrowArrayStream stream)
    {
        // ArrowStreamReader (and ArrowFileReader) can read synchronously, so a reader over a network stream does not
        // block a thread on an asynchronous read.
        if (stream is ArrowStreamReader reader)
        {
            return reader.ReadNextRecordBatch();
        }

        var next = stream.ReadNextRecordBatchAsync();
        return next.IsCompletedSuccessfully ? next.Result : next.AsTask().GetAwaiter().GetResult();
    }

    private void EnsureUsable()
    {
        if (closed)
        {
            throw new InvalidOperationException("Appender is already closed");
        }

        if (isFaulted)
        {
            throw new InvalidOperationException("The appender cannot be reused after a batch failed");
        }
    }
}
