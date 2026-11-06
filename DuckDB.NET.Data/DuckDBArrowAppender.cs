using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;

namespace DuckDB.NET.Data;

/// <summary>
/// Appends Apache Arrow record batches to a DuckDB table through DuckDB's Arrow C Data Interface
/// (<c>duckdb_schema_from_arrow</c> / <c>duckdb_data_chunk_from_arrow</c>), with no row-by-row marshaling.
/// </summary>
/// <remarks>
/// Columns are matched by position: a batch must have as many columns as the table, and field names are
/// ignored. DuckDB converts each Arrow column and casts it to the table column type when the types differ,
/// so unlike <see cref="DuckDBAppender"/> this path is type-checked: a value that cannot be cast is an error.
/// <para>
/// The appended data is copied, so a batch can be disposed as soon as the call returns. Data is written to
/// the table when the appender flushes, closes or is disposed. Instances are not thread-safe.
/// </para>
/// <para>
/// If DuckDB fails to convert or append a batch, none of that batch is appended and the appender is faulted:
/// no more batches can be appended. The batches appended before the failure are still written when the
/// appender is closed or disposed. Wrap the appends in a transaction if you need all-or-nothing.
/// </para>
/// <para>
/// To copy a query result, read it on another connection: a connection with an open streaming result cannot
/// run the appender's work.
/// </para>
/// </remarks>
public sealed class DuckDBArrowAppender : IDisposable
{
    private readonly Native.DuckDBAppender nativeAppender;
    private readonly DuckDBNativeConnection connection;
    private readonly int columnCount;

    // Batches of one stream share a Schema instance, so caching the last conversion converts a stream once.
    private Schema? convertedArrowSchema;
    private DuckDBArrowConvertedSchema? convertedSchema;

    private bool closed;
    private bool isFaulted;

    internal DuckDBArrowAppender(Native.DuckDBAppender appender, DuckDBNativeConnection connection)
    {
        nativeAppender = appender;
        this.connection = connection;
        columnCount = (int)NativeMethods.Appender.DuckDBAppenderColumnCount(nativeAppender);
    }

    /// <summary>
    /// Appends every row of <paramref name="batch"/>.
    /// </summary>
    /// <exception cref="ArgumentException">The batch does not have as many columns as the table.</exception>
    /// <exception cref="DuckDBException">DuckDB cannot convert the schema, or cannot convert or append the
    /// data. A failure after the schema is converted faults the appender.</exception>
    public void AppendBatch(RecordBatch batch)
    {
        ArgumentNullException.ThrowIfNull(batch);
        EnsureUsable();

        AppendBatchCore(batch);
    }

    /// <summary>
    /// Reads <paramref name="stream"/> to the end and appends every batch. Each batch read from the stream is
    /// disposed after it is appended; the stream itself is not disposed.
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
    /// Reads <paramref name="stream"/> to the end and appends every batch. Each batch read from the stream is
    /// disposed after it is appended; the stream itself is not disposed.
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
    /// Drops the data appended since the appender last flushed.
    /// </summary>
    public void Clear()
    {
        EnsureUsable();

        var state = NativeMethods.Appender.DuckDBAppenderClear(nativeAppender);
        if (!state.IsSuccess())
        {
            NativeMethods.Appender.DuckDBAppenderErrorData(nativeAppender).ThrowOnError();
        }
    }

    public void Close()
    {
        // A faulted appender can still be closed so the data appended before the failure is written.
        if (closed)
        {
            throw new InvalidOperationException("Appender is already closed");
        }

        closed = true;

        try
        {
            var state = NativeMethods.Appender.DuckDBAppenderClose(nativeAppender);
            if (!state.IsSuccess())
            {
                NativeMethods.Appender.DuckDBAppenderErrorData(nativeAppender).ThrowOnError();
            }
        }
        finally
        {
            try
            {
                convertedSchema?.Dispose();
            }
            finally
            {
                nativeAppender.Close();
            }
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

        // RecordBatch does not check that its columns are as long as the batch, and DuckDB v2.0.0 does not check it
        // either (the check came after 2.0.0): a shorter column is read past its end, storing garbage or crashing.
        for (var index = 0; index < batch.ColumnCount; index++)
        {
            var length = batch.Column(index).Length;
            if (length != batch.Length)
            {
                throw new ArgumentException($"Column {index} has {length} rows but the batch has {batch.Length}.", nameof(batch));
            }
        }

        var schema = GetConvertedSchema(batch.Schema);

        if (batch.Length == 0)
        {
            return;
        }

        try
        {
            // The whole batch goes to DuckDB as one chunk, whatever its length. That needs an engine with
            // duckdb/duckdb#27664: DuckDB v2.0.0 cannot cast a chunk of more than 2048 rows.
            AppendChunk(batch, schema);
        }
        catch
        {
            isFaulted = true;
            throw;
        }
    }

    private unsafe DuckDBArrowConvertedSchema GetConvertedSchema(Schema schema)
    {
        if (convertedSchema is not null && ReferenceEquals(schema, convertedArrowSchema))
        {
            return convertedSchema;
        }

        var cSchema = CArrowSchema.Create();

        try
        {
            CArrowSchemaExporter.ExportSchema(schema, cSchema);

            var error = NativeMethods.Arrow.DuckDBSchemaFromArrow(connection, (IntPtr)cSchema, out var converted);

            try
            {
                error.ThrowOnError("Failed to convert the Arrow schema to a DuckDB schema.");
            }
            catch
            {
                converted.Dispose();
                throw;
            }

            convertedSchema?.Dispose();
            convertedSchema = converted;
            convertedArrowSchema = schema;

            return converted;
        }
        finally
        {
            // DuckDB does not take ownership of the schema; Free releases it.
            CArrowSchema.Free(cSchema);
        }
    }

    private unsafe void AppendChunk(RecordBatch batch, DuckDBArrowConvertedSchema schema)
    {
        var cArray = CArrowArray.Create();

        try
        {
            CArrowArrayExporter.ExportRecordBatch(batch, cArray);

            var error = NativeMethods.Arrow.DuckDBDataChunkFromArrow(connection, (IntPtr)cArray, schema, out var chunk);

            // Destroying the chunk releases the exported array, which unpins the batch's managed buffers.
            using (chunk)
            {
                error.ThrowOnError("Failed to convert an Arrow record batch to a DuckDB data chunk.");

                var state = NativeMethods.Appender.DuckDBAppendDataChunk(nativeAppender, chunk);
                if (!state.IsSuccess())
                {
                    NativeMethods.Appender.DuckDBAppenderErrorData(nativeAppender).ThrowOnError();
                }
            }
        }
        finally
        {
            // DuckDB takes ownership of the array (it sets release to NULL) unless an argument is null, and newer
            // engines also refuse a malformed array without taking it. Free releases the array only if DuckDB did
            // not take it, and frees the struct either way.
            CArrowArray.Free(cArray);
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
