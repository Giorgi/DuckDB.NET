using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using DuckDB.NET.Data.Execution;

namespace DuckDB.NET.Data.Arrow;

/// <summary>
/// Streams the rows of a DuckDB query result as Apache Arrow <see cref="RecordBatch"/> values. DuckDB produces the
/// arrays itself (<c>statement_execute_arrow</c> of the C API V2), in batches of its default size, and each is imported
/// with no row-by-row marshaling. A failure while the query runs is a <see cref="DuckDBException"/> from
/// <see cref="ReadNextRecordBatchAsync"/>, with the exception of a C# function as its inner exception.
/// </summary>
internal sealed unsafe class DuckDBArrowArrayStream : IArrowArrayStream
{
    private readonly DuckDBV2ArrowResult result;
    private bool disposed;
    private bool ended;

    public Schema Schema { get; }

    internal DuckDBArrowArrayStream(DuckDBV2ArrowResult result)
    {
        this.result = result;

        var schema = result.GetSchema();
        try
        {
            Schema = CArrowSchemaImporter.ImportSchema(schema);
        }
        finally
        {
            CArrowSchema.Free(schema);
        }
    }

    public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        if (cancellationToken.IsCancellationRequested)
        {
            return new ValueTask<RecordBatch?>(Task.FromCanceled<RecordBatch?>(cancellationToken));
        }

        if (ended)
        {
            return new ValueTask<RecordBatch?>((RecordBatch?)null);
        }

        var array = result.FetchArray();

        if (array == null)
        {
            ended = true;
            return new ValueTask<RecordBatch?>((RecordBatch?)null);
        }

        try
        {
            return new ValueTask<RecordBatch?>(CArrowArrayImporter.ImportRecordBatch(array, Schema));
        }
        finally
        {
            CArrowArray.Free(array);
        }
    }

    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        disposed = true;
        result.Dispose();
    }
}
