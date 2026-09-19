using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;

namespace DuckDB.NET.Data.Arrow;

/// <summary>
/// Streams the rows of a DuckDB query result as Apache Arrow <see cref="RecordBatch"/> values using
/// DuckDB's Arrow C Data Interface (<c>duckdb_to_arrow_schema</c> / <c>duckdb_data_chunk_to_arrow</c>).
/// Each DuckDB data chunk is converted into one Arrow record batch and imported with no row-by-row marshaling.
/// </summary>
internal sealed class DuckDBArrowArrayStream : IArrowArrayStream
{
    private DuckDBResult result;
    private readonly DuckDBArrowOptions arrowOptions;
    private readonly bool streaming;
    private bool disposed;

    public Schema Schema { get; }

    internal DuckDBArrowArrayStream(DuckDBResult result)
    {
        this.result = result;

        arrowOptions = NativeMethods.Arrow.DuckDBResultGetArrowOptions(ref this.result);
        if (arrowOptions.IsInvalid)
        {
            this.result.Close();
            throw new InvalidOperationException("Failed to obtain Arrow options from the DuckDB result.");
        }

        streaming = NativeMethods.Types.DuckDBResultIsStreaming(this.result) > 0;

        try
        {
            Schema = BuildSchema();
        }
        catch
        {
            arrowOptions.Dispose();
            this.result.Close();
            throw;
        }
    }

    private unsafe Schema BuildSchema()
    {
        var columnCount = NativeMethods.Query.DuckDBColumnCount(ref result);

        var logicalTypes = new DuckDBLogicalType[columnCount];
        var typeHandles = new IntPtr[columnCount];
        var namePointers = new IntPtr[columnCount];

        try
        {
            for (var index = 0UL; index < columnCount; index++)
            {
                var logicalType = NativeMethods.Query.DuckDBColumnLogicalType(ref result, (long)index);
                logicalTypes[index] = logicalType;
                typeHandles[index] = logicalType.DangerousGetHandle();

                var name = NativeMethods.Query.DuckDBColumnName(ref result, (long)index);
                namePointers[index] = Marshal.StringToCoTaskMemUTF8(name);
            }

            var cSchema = CArrowSchema.Create();

            try
            {
                fixed (IntPtr* typesPointer = typeHandles)
                fixed (IntPtr* namesPointer = namePointers)
                {
                    var error = NativeMethods.Arrow.DuckDBToArrowSchema(arrowOptions, (IntPtr)typesPointer, (IntPtr)namesPointer, columnCount, (IntPtr)cSchema);
                    error.ThrowOnError("Failed to convert the DuckDB result schema to an Arrow schema.");
                }

                return CArrowSchemaImporter.ImportSchema(cSchema);
            }
            finally
            {
                CArrowSchema.Free(cSchema);
            }
        }
        finally
        {
            foreach (var pointer in namePointers)
            {
                if (pointer != IntPtr.Zero)
                {
                    Marshal.FreeCoTaskMem(pointer);
                }
            }

            foreach (var logicalType in logicalTypes)
            {
                logicalType?.Dispose();
            }
        }
    }

    public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        if (cancellationToken.IsCancellationRequested)
        {
            return new ValueTask<RecordBatch?>(Task.FromCanceled<RecordBatch?>(cancellationToken));
        }

        var chunk = streaming
            ? NativeMethods.StreamingResult.DuckDBStreamFetchChunk(result)
            : NativeMethods.Query.DuckDBFetchChunk(result);

        if (chunk.IsInvalid)
        {
            chunk.Dispose();
            return new ValueTask<RecordBatch?>((RecordBatch?)null);
        }

        try
        {
            return new ValueTask<RecordBatch?>(ConvertChunk(chunk));
        }
        finally
        {
            chunk.Dispose();
        }
    }

    private unsafe RecordBatch ConvertChunk(DuckDBDataChunk chunk)
    {
        var cArray = CArrowArray.Create();

        try
        {
            var error = NativeMethods.Arrow.DuckDBDataChunkToArrow(arrowOptions, chunk, (IntPtr)cArray);
            error.ThrowOnError("Failed to convert a DuckDB data chunk to an Arrow array.");

            return CArrowArrayImporter.ImportRecordBatch(cArray, Schema);
        }
        finally
        {
            CArrowArray.Free(cArray);
        }
    }

    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        disposed = true;

        arrowOptions.Dispose();
        result.Close();
    }
}
