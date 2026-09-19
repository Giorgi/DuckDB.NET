using DuckDB.NET.Data.Common;
using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Data.Extensions;
namespace DuckDB.NET.Data;

/// <summary>
/// Appends rows to a DuckDB table.
/// </summary>
/// <remarks>
/// Instances are not thread-safe. Do not call other methods on the same appender from an
/// <see cref="AppendRow{TState}(TState, Action{IDuckDBAppenderRow, TState})"/> callback.
/// </remarks>
public class DuckDBAppender : IDisposable
{
    private bool closed;
    private bool isAppendingRow;
    private bool isFaulted;
    private readonly Native.DuckDBAppender nativeAppender;
    private readonly string qualifiedTableName;

    private ulong rowCount;

    private readonly DuckDBLogicalType[] logicalTypes;
    private readonly DuckDBDataChunk dataChunk;
    private readonly VectorDataWriterBase[] vectorWriters;
    private DuckDBAppenderRow? reusableRow;

    internal DuckDBAppender(Native.DuckDBAppender appender, string qualifiedTableName)
    {
        nativeAppender = appender;
        this.qualifiedTableName = qualifiedTableName;

        var columnCount = NativeMethods.Appender.DuckDBAppenderColumnCount(nativeAppender);

        vectorWriters = new VectorDataWriterBase[columnCount];
        logicalTypes = new DuckDBLogicalType[columnCount];
        var logicalTypeHandles = new IntPtr[columnCount];

        for (ulong index = 0; index < columnCount; index++)
        {
            logicalTypes[index] = NativeMethods.Appender.DuckDBAppenderColumnType(nativeAppender, index);
            logicalTypeHandles[index] = logicalTypes[index].DangerousGetHandle();
        }

        dataChunk = NativeMethods.DataChunks.DuckDBCreateDataChunk(logicalTypeHandles, columnCount);
    }

    /// <summary>
    /// Gets the logical types of the columns in the appender.
    /// </summary>
    internal IReadOnlyList<DuckDBLogicalType> LogicalTypes => logicalTypes;

    /// <summary>
    /// Creates an independent row. The caller must append every column and call
    /// <see cref="IDuckDBAppenderRow.EndRow"/>.
    /// </summary>
    /// <remarks>
    /// A new row instance is allocated on every call. Prefer
    /// <see cref="AppendRow{TState}(TState, Action{IDuckDBAppenderRow, TState})"/>, which reuses a
    /// single row instance and avoids the per-row allocation; use this method only when you need an
    /// independent row instance whose lifetime you control.
    /// </remarks>
    public IDuckDBAppenderRow CreateRow()
    {
        EnsureUsable();
        return new DuckDBAppenderRow(qualifiedTableName, vectorWriters, PrepareRow(), dataChunk, nativeAppender);
    }

    /// <summary>
    /// Appends a complete row using a reusable row instance.
    /// </summary>
    /// <param name="writeRow">A callback that appends every column value. This method calls
    /// <see cref="IDuckDBAppenderRow.EndRow"/> after the callback returns.</param>
    /// <remarks>
    /// The row is valid only during the callback and must not be retained. The callback must not
    /// call other methods on this appender. If the callback or automatic
    /// <see cref="IDuckDBAppenderRow.EndRow"/> fails, the failed row is discarded and no more rows
    /// can be appended; the rows completed before the failure are still written when the appender
    /// is closed or disposed. Wrap the append in a transaction if you need all-or-nothing.
    /// <para>
    /// For large or hot-path loads, prefer the
    /// <see cref="AppendRow{TState}(TState, Action{IDuckDBAppenderRow, TState})"/> overload with a
    /// <see langword="static"/> callback. A callback passed here that captures variables allocates a
    /// closure on every call; passing the captured data as the <c>state</c> argument to that overload
    /// keeps the append allocation-free.
    /// </para>
    /// </remarks>
    public void AppendRow(Action<IDuckDBAppenderRow> writeRow)
    {
        ArgumentNullException.ThrowIfNull(writeRow);

        // Pass the callback as state so the adapter remains static and allocation-free.
        AppendRow(writeRow, static (row, callback) => callback(row));
    }

    /// <summary>
    /// Appends a complete row using a reusable row instance without exposing that instance as a
    /// return value. The row passed to <paramref name="writeRow"/> is only valid for the duration of
    /// the callback and must not be retained or used after the callback returns.
    /// </summary>
    /// <typeparam name="TState">The type of value used to populate the row.</typeparam>
    /// <param name="state">The value used to populate the row.</param>
    /// <param name="writeRow">A callback that appends every column value. This method calls
    /// <see cref="IDuckDBAppenderRow.EndRow"/> after the callback returns.</param>
    /// <remarks>
    /// The callback must not call other methods on this appender. If the callback or automatic
    /// <see cref="IDuckDBAppenderRow.EndRow"/> fails, the failed row is discarded and no more rows
    /// can be appended; the rows completed before the failure are still written when the appender
    /// is closed or disposed. Wrap the append in a transaction if you need all-or-nothing.
    /// <para>
    /// This overload is the allocation-free choice for hot paths: use a <see langword="static"/>
    /// callback and pass any per-row data through <paramref name="state"/> so the callback captures
    /// nothing and no closure is allocated per row.
    /// </para>
    /// </remarks>
    public void AppendRow<TState>(TState state, Action<IDuckDBAppenderRow, TState> writeRow)
    {
        ArgumentNullException.ThrowIfNull(writeRow);
        EnsureUsable();

        DuckDBAppenderRow? row = null;
        isAppendingRow = true;

        try
        {
            row = CreateReusableRow();
            writeRow(row, state);
            row.EndRow();
        }
        catch
        {
            if (row is not null)
            {
                DiscardFailedAppendRow(row);
            }

            throw;
        }
        finally
        {
            isAppendingRow = false;
        }
    }

    /// <summary>
    /// Creates a row whose instance may be reused by the next call. This is only safe for internal
    /// callers that create, populate and end each row without exposing the row reference.
    /// </summary>
    internal DuckDBAppenderRow CreateReusableRow()
    {
        var rowIndex = PrepareRow();

        if (reusableRow is null)
        {
            reusableRow = new DuckDBAppenderRow(qualifiedTableName, vectorWriters, rowIndex, dataChunk, nativeAppender);
        }
        else
        {
            reusableRow.Reset(rowIndex);
        }

        return reusableRow;
    }

    private ulong PrepareRow()
    {
        if (rowCount % DuckDBGlobalData.VectorSize == 0)
        {
            AppendDataChunk();

            InitVectorWriters();

            rowCount = 0;
        }

        rowCount++;
        return rowCount - 1;
    }

    public void Clear()
    {
        EnsureUsable();

        var state = NativeMethods.Appender.DuckDBAppenderClear(nativeAppender);
        if (!state.IsSuccess())
        {
            NativeMethods.Appender.DuckDBAppenderErrorData(nativeAppender).ThrowOnError();
        }

        rowCount = 0;
        NativeMethods.DataChunks.DuckDBDataChunkReset(dataChunk);
        InitVectorWriters();
    }

    public void Close()
    {
        // A faulted appender can still be closed so the rows completed before the failure are
        // written; only further appends are rejected (see EnsureUsable).
        EnsureNotAppendingRow();

        if (closed)
        {
            throw new InvalidOperationException("Appender is already closed");
        }

        CloseCore();
    }

    private void CloseCore()
    {
        closed = true;

        try
        {
            AppendDataChunk();

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
                DisposeManagedResources();
            }
            finally
            {
                nativeAppender.Close();
            }
        }
    }

    private void DisposeManagedResources()
    {
        foreach (var logicalType in logicalTypes)
        {
            logicalType.Dispose();
        }

        foreach (var writer in vectorWriters)
        {
            writer?.Dispose();
        }

        dataChunk.Dispose();
    }

    public void Dispose()
    {
        if (!closed)
        {
            Close();
        }
    }

    private void InitVectorWriters()
    {
        for (long index = 0; index < vectorWriters.LongLength; index++)
        {
            var vector = NativeMethods.DataChunks.DuckDBDataChunkGetVector(dataChunk, index);

            vectorWriters[index]?.Dispose();
            vectorWriters[index] = VectorDataWriterFactory.CreateWriter(vector, logicalTypes[index]);
        }
    }

    private void AppendDataChunk()
    {
        NativeMethods.DataChunks.DuckDBDataChunkSetSize(dataChunk, rowCount);
        var state = NativeMethods.Appender.DuckDBAppendDataChunk(nativeAppender, dataChunk);

        if (!state.IsSuccess())
        {
            NativeMethods.Appender.DuckDBAppenderErrorData(nativeAppender).ThrowOnError();
        }

        NativeMethods.DataChunks.DuckDBDataChunkReset(dataChunk);
    }

    private void DiscardFailedAppendRow(DuckDBAppenderRow row)
    {
        // The row index is also the number of completed rows before the failed row in this chunk,
        // so resetting rowCount drops the partial row without touching the completed ones. The
        // completed rows stay buffered and are written only when the caller closes or disposes the
        // appender; the failure itself never commits.
        rowCount = row.ChunkRowIndex;
        row.Invalidate();
        isFaulted = true;
    }

    private void EnsureNotAppendingRow()
    {
        if (isAppendingRow)
        {
            throw new InvalidOperationException("The appender cannot be used from inside an AppendRow callback");
        }
    }

    private void EnsureUsable()
    {
        EnsureNotAppendingRow();

        if (isFaulted)
        {
            throw new InvalidOperationException("The appender cannot be reused after an AppendRow callback failed");
        }

        if (closed)
        {
            throw new InvalidOperationException("Appender is already closed");
        }
    }
}
