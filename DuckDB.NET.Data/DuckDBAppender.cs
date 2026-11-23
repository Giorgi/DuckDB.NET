using DuckDB.NET.Data.Common;
using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Data.Execution;
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
    private readonly string qualifiedTableName;

    private ulong rowCount;

    // On the C API V2 the rows go into a buffer that is inserted into the table when it is flushed.
    private readonly DuckDBV2AppendBuffer buffer;
    private readonly DuckDBType[] columnTypes;
    private readonly VectorDataWriterBase[] vectorWriters;
    private DuckDBAppenderRow? reusableRow;
    private DuckDBAppenderRow? currentRow;

    // Per row of the chunk, which columns it leaves to their defaults (DuckDBV2AppendBuffer.GetPattern); 0 for none.
    private readonly int[] rowPatterns = new int[DuckDBGlobalData.VectorSize];
    private readonly bool[] defaultedColumns;
    private bool rowHasDefaults;

    internal unsafe DuckDBAppender(DuckDBV2AppendBuffer buffer, string qualifiedTableName)
    {
        this.buffer = buffer;
        this.qualifiedTableName = qualifiedTableName;

        vectorWriters = new VectorDataWriterBase[buffer.ColumnCount];
        defaultedColumns = new bool[buffer.ColumnCount];
        columnTypes = Array.ConvertAll(buffer.ColumnTypes, type =>
        {
            using var lease = type.Acquire();
            return DuckDBV2LogicalTypes.GetId(lease.Pointer).ToDuckDBType();
        });
    }

    /// <summary>The types of the table's columns, for the checks of the mapped appender.</summary>
    internal IReadOnlyList<DuckDBType> ColumnTypes => columnTypes;

    /// <summary>
    /// Creates an independent row. The caller must append every column and call
    /// <see cref="IDuckDBAppenderRow.EndRow"/>.
    /// </summary>
    /// <remarks>
    /// A row is written once every column has a value. If appending a value throws, append the value
    /// again to finish the row. A row left with missing values is discarded: creating the next row then
    /// throws and no more rows can be appended, while the rows completed before it are still written
    /// when the appender is closed or disposed. A row still incomplete when the appender is closed,
    /// disposed or cleared is discarded without an error.
    /// <para>
    /// A new row instance is allocated on every call. Prefer
    /// <see cref="AppendRow{TState}(TState, Action{IDuckDBAppenderRow, TState})"/>, which reuses a
    /// single row instance and avoids the per-row allocation; use this method only when you need an
    /// independent row instance whose lifetime you control.
    /// </para>
    /// </remarks>
    public IDuckDBAppenderRow CreateRow()
    {
        EnsureUsable();
        EnsurePreviousRowComplete();
        currentRow = new DuckDBAppenderRow(qualifiedTableName, vectorWriters, PrepareRow(), this);
        return currentRow;
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
        EnsurePreviousRowComplete();

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
            reusableRow = new DuckDBAppenderRow(qualifiedTableName, vectorWriters, rowIndex, this);
        }
        else
        {
            reusableRow.Reset(rowIndex);
        }

        currentRow = reusableRow;
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

        if (rowHasDefaults)
        {
            Array.Clear(defaultedColumns);
            rowHasDefaults = false;
        }

        rowPatterns[rowCount] = 0;
        rowCount++;
        return rowCount - 1;
    }

    // The row leaves the column to its default. The cell holds a NULL, which the insert does not read.
    internal void MarkDefault(int column, ulong rowIndex)
    {
        defaultedColumns[column] = true;
        rowHasDefaults = true;
        rowPatterns[rowIndex] = buffer.GetPattern(defaultedColumns);
    }

    public void Clear()
    {
        EnsureUsable();

        buffer.Clear();

        DiscardIncompleteRow();

        rowCount = 0;
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
            DiscardIncompleteRow();
            AppendDataChunk();
            buffer.Flush();
        }
        finally
        {
            try
            {
                DisposeManagedResources();
            }
            finally
            {
                buffer.Dispose();
            }
        }
    }

    private void DisposeManagedResources()
    {
        foreach (var writer in vectorWriters)
        {
            writer?.Dispose();
        }
    }

    public void Dispose()
    {
        if (!closed)
        {
            Close();
        }
    }

    // The chunk is emptied after it is buffered, and its vectors change, so the writers are made again.
    private unsafe void InitVectorWriters()
    {
        for (var index = 0; index < vectorWriters.Length; index++)
        {
            vectorWriters[index]?.Dispose();

            using var lease = buffer.ColumnTypes[index].Acquire();
            vectorWriters[index] = VectorDataWriterFactory.CreateWriter(buffer.GetVector(index), lease.Pointer);
        }
    }

    // Buffers the rows of the chunk, which flushes the buffer every DuckDBV2AppendBuffer.FlushThreshold rows.
    private void AppendDataChunk() => buffer.BufferChunk(rowCount, rowPatterns.AsSpan(0, (int)rowCount));

    private void DiscardFailedAppendRow(DuckDBAppenderRow row)
    {
        // The completed rows stay buffered and are written only when the caller closes or disposes
        // the appender; the failure itself never commits.
        DiscardRow(row);
        isFaulted = true;
    }

    // https://github.com/Giorgi/DuckDB.NET/issues/355
    // A row counts toward the chunk as soon as it is created, so a row left with missing values would
    // be written with its unwritten slots holding garbage: a crash for strings and lists, a junk value
    // otherwise. Moving on from such a row faults the appender, as a failed AppendRow does, so the
    // incomplete row is always the last one and its slot is never reused.
    private void EnsurePreviousRowComplete()
    {
        if (currentRow is not { IsComplete: false } row)
        {
            return;
        }

        var message = $"The previous row has only {row.ValueCount} of {vectorWriters.Length} values, so it was discarded. The appender cannot be reused after a row is left incomplete";

        DiscardRow(row);
        isFaulted = true;

        throw new InvalidOperationException(message);
    }

    private void DiscardIncompleteRow()
    {
        if (currentRow is { IsComplete: false })
        {
            DiscardRow(currentRow);
        }

        currentRow = null;
    }

    private void DiscardRow(DuckDBAppenderRow row)
    {
        // The row index is also the number of completed rows before this row in the chunk, so
        // resetting rowCount drops the row without touching the completed ones.
        rowCount = row.ChunkRowIndex;
        row.Invalidate();
        currentRow = null;
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
            throw new InvalidOperationException("The appender cannot be reused after a row failed");
        }

        if (closed)
        {
            throw new InvalidOperationException("Appender is already closed");
        }
    }
}
