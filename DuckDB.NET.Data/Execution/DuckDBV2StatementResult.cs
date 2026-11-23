using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Execution;

/// <summary>
/// The streaming result of one statement. V2 executes lazily: the statement runs while its result is read, an
/// undrained result applies none of its effects, and destroying a result before its end aborts the statement.
/// </summary>
internal sealed unsafe class DuckDBV2StatementResult : IDisposable
{
    private readonly DuckDBV2Result result;

    // The statement runs while its chunks are fetched, so the callbacks of C# functions run then too.
    private readonly DuckDBV2Connection connection;

    // The first chunk, fetched before the reader asked for it to learn the result's shape.
    private DuckDBV2DataChunk? pendingChunk;

    public DuckDBV2StatementResult(DuckDBV2Result result, DuckDBStatementType statementType, DuckDBV2Connection connection)
    {
        this.result = result;
        this.connection = connection;
        StatementType = statementType;

        if (TryGetResultType(out var resultType))
        {
            ResultType = resultType;
            return;
        }

        // A statement that DuckDB expands into several (a dynamic PIVOT) knows its shape only once stepping has bound
        // the statement that produces rows, or the group has ended without one. Fetch the first chunk to get there,
        // and keep it for the reader.
        pendingChunk = FetchChunkCore();

        if (!TryGetResultType(out resultType))
        {
            throw new DuckDBException("The shape of the result is still not known after its first chunk", DuckDBErrorCode.InputMetadataDeferred);
        }

        ResultType = resultType;
    }

    private bool TryGetResultType(out DUCKDB_V2_RESULT_TYPE resultType)
    {
        using var lease = result.Acquire();
        _duckdb_v2_error_info* error = null;
        DUCKDB_V2_RESULT_TYPE type;
        var code = NativeMethodsV2.duckdb_v2_result_get_result_type(lease.Pointer, &type, &error);

        if (code == DUCKDB_V2_ERROR.INPUT_METADATA_DEFERRED)
        {
            NativeMethodsV2.duckdb_v2_error_info_destroy(&error);
            resultType = default;
            return false;
        }

        code.ThrowIfFailed(ref error);
        resultType = type;
        return true;
    }

    // Taken from the parsed statement: the result of a prepared statement reports EXECUTE.
    public DuckDBStatementType StatementType { get; }

    public DUCKDB_V2_RESULT_TYPE ResultType { get; }

    public bool ReturnsRows => ResultType == DUCKDB_V2_RESULT_TYPE.QUERY_RESULT;

    /// <summary>Whether the statement has run to its end: every chunk was fetched, or the result was drained.</summary>
    public bool Ended { get; private set; }

    /// <summary>The rows of the chunks fetched so far.</summary>
    public long RowsFetched { get; private set; }

    /// <summary>The next chunk, or null at the end of the result. Throws when producing the chunk failed.</summary>
    public DuckDBV2DataChunk? FetchChunk()
    {
        if (pendingChunk is { } chunk)
        {
            pendingChunk = null;
            return chunk;
        }

        return FetchChunkCore();
    }

    private DuckDBV2DataChunk? FetchChunkCore()
    {
        if (Ended)
        {
            return null;
        }

        using var lease = result.Acquire();
        using var scope = DuckDBV2CallbackScope.Enter(connection);
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_data_chunk* chunk;
        NativeMethodsV2.duckdb_v2_result_fetch_chunk(lease.Pointer, &chunk, &error).ThrowIfFailed(ref error, connection, "Fetching the result failed");

        if (chunk == null)
        {
            Ended = true;
            return null;
        }

        var dataChunk = new DuckDBV2DataChunk(chunk);
        RowsFetched += (long)GetSize(dataChunk);
        return dataChunk;
    }

    /// <summary>
    /// Runs the statement to its end and returns the rows it affected: the rows changed for a statement that returns
    /// none, all the rows of one that does (an INSERT ... RETURNING), as V1 reported them.
    /// </summary>
    public long Finish()
    {
        if (Ended && pendingChunk is null)
        {
            return RowsFetched;
        }

        if (ReturnsRows || Ended)
        {
            while (FetchChunk() is { } chunk)
            {
                chunk.Dispose();
            }

            return RowsFetched;
        }

        using var lease = result.Acquire();
        using var scope = DuckDBV2CallbackScope.Enter(connection);
        _duckdb_v2_error_info* error = null;
        ulong rowsChanged;
        NativeMethodsV2.duckdb_v2_result_drain(lease.Pointer, &rowsChanged, &error).ThrowIfFailed(ref error, connection, "Executing the statement failed");

        Ended = true;
        RowsFetched = (long)rowsChanged;
        return RowsFetched;
    }

    /// <summary>
    /// Leaves the result before reading all of it, and returns the rows it affected. A SELECT is aborted, which
    /// costs nothing and changes nothing. Any other statement is run to its end, so that what it writes is applied
    /// as it was with V1, where results were materialized when the statement executed.
    /// </summary>
    public long Leave() => Ended || StatementType == DuckDBStatementType.Select ? RowsFetched : Finish();

    /// <summary>The names and types of the result's columns, valid until the schema is disposed.</summary>
    public Schema GetSchema()
    {
        using var lease = result.Acquire();
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_schema* schema;
        NativeMethodsV2.duckdb_v2_result_get_schema(lease.Pointer, &schema, &error).ThrowIfFailed(ref error);

        return new Schema(schema);
    }

    public static ulong GetSize(DuckDBV2DataChunk chunk)
    {
        using var lease = chunk.Acquire();
        _duckdb_v2_error_info* error = null;
        ulong size;
        NativeMethodsV2.duckdb_v2_data_chunk_get_size(lease.Pointer, &size, &error).ThrowIfFailed(ref error);
        return size;
    }

    public void Dispose()
    {
        pendingChunk?.Dispose();
        result.Dispose();
    }

    public readonly struct Schema(_duckdb_v2_schema* schema) : IDisposable
    {
        public int Count
        {
            get
            {
                _duckdb_v2_error_info* error = null;
                ulong count;
                NativeMethodsV2.duckdb_v2_schema_get_count(schema, &count, &error).ThrowIfFailed(ref error);
                return checked((int)count);
            }
        }

        /// <summary>The name and the borrowed logical type of a column.</summary>
        public (string Name, IntPtr LogicalType) GetField(int index)
        {
            _duckdb_v2_error_info* error = null;
            sbyte* name;
            ulong nameLength;
            _duckdb_v2_logical_type* type;
            NativeMethodsV2.duckdb_v2_schema_get_field(schema, (ulong)index, &name, &nameLength, &type, &error).ThrowIfFailed(ref error);

            return (DuckDBV2Utf8.ToManaged(name, nameLength), (IntPtr)type);
        }

        public void Dispose()
        {
            var pointer = schema;
            NativeMethodsV2.duckdb_v2_schema_destroy(&pointer);
        }
    }
}
