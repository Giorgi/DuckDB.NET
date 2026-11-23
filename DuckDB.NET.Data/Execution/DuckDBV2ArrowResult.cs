using Apache.Arrow.C;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Execution;

/// <summary>
/// The result of one statement executed for Arrow (<c>statement_execute_arrow</c>). Like
/// <see cref="DuckDBV2StatementResult"/> it streams: the statement runs while its arrays are fetched.
/// </summary>
internal sealed unsafe class DuckDBV2ArrowResult : IDisposable
{
    private readonly DuckDBV2ArrowResultHandle result;

    // The first array, fetched to learn the shape of a statement DuckDB expands into several (a dynamic PIVOT).
    private CArrowArray* pendingArray;

    public DuckDBV2ArrowResult(DuckDBV2ArrowResultHandle result, DuckDBV2Connection connection)
    {
        this.result = result;
        Connection = connection;

        if (TryGetResultType(out var resultType))
        {
            ReturnsRows = resultType == DUCKDB_V2_RESULT_TYPE.QUERY_RESULT;
            return;
        }

        pendingArray = FetchCore();

        if (!TryGetResultType(out resultType))
        {
            throw new DuckDBException("The shape of the result is still not known after its first array", DuckDBErrorCode.InputMetadataDeferred);
        }

        ReturnsRows = resultType == DUCKDB_V2_RESULT_TYPE.QUERY_RESULT;
    }

    public DuckDBV2Connection Connection { get; }

    public bool ReturnsRows { get; }

    private bool TryGetResultType(out DUCKDB_V2_RESULT_TYPE resultType)
    {
        using var lease = result.Acquire();
        _duckdb_v2_error_info* error = null;
        DUCKDB_V2_RESULT_TYPE type;
        var code = NativeMethodsV2.duckdb_v2_arrow_result_get_result_type(lease.Pointer, &type, &error);

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

    /// <summary>The Arrow schema of the arrays. The caller frees it with <see cref="CArrowSchema.Free"/>.</summary>
    public CArrowSchema* GetSchema()
    {
        var schema = CArrowSchema.Create();

        try
        {
            using var lease = result.Acquire();
            _duckdb_v2_error_info* error = null;
            NativeMethodsV2.duckdb_v2_arrow_result_get_schema(lease.Pointer, (ArrowSchema*)schema, &error).ThrowIfFailed(ref error, Connection);
            return schema;
        }
        catch
        {
            CArrowSchema.Free(schema);
            throw;
        }
    }

    /// <summary>
    /// The next array, or null at the end of the result. The caller frees it with <see cref="CArrowArray.Free"/>.
    /// Throws when producing it failed.
    /// </summary>
    public CArrowArray* FetchArray()
    {
        if (pendingArray != null)
        {
            var array = pendingArray;
            pendingArray = null;
            return array;
        }

        return FetchCore();
    }

    private CArrowArray* FetchCore()
    {
        var array = CArrowArray.Create();

        try
        {
            using var lease = result.Acquire();
            using var scope = DuckDBV2CallbackScope.Enter(Connection);
            _duckdb_v2_error_info* error = null;
            NativeMethodsV2.duckdb_v2_arrow_result_fetch_array(lease.Pointer, (ArrowArray*)array, &error).ThrowIfFailed(ref error, Connection, "Fetching the result failed");
        }
        catch
        {
            CArrowArray.Free(array);
            throw;
        }

        // At the end of the result the array has no release callback.
        if (((ArrowArray*)array)->release == null)
        {
            CArrowArray.Free(array);
            return null;
        }

        return array;
    }

    /// <summary>Runs a statement that returns no rows to its end.</summary>
    public void Drain()
    {
        using var lease = result.Acquire();
        using var scope = DuckDBV2CallbackScope.Enter(Connection);
        _duckdb_v2_error_info* error = null;
        ulong rowsChanged;
        NativeMethodsV2.duckdb_v2_arrow_result_drain(lease.Pointer, &rowsChanged, &error).ThrowIfFailed(ref error, Connection, "Executing the statement failed");
    }

    public void Dispose()
    {
        if (pendingArray != null)
        {
            CArrowArray.Free(pendingArray);
            pendingArray = null;
        }

        result.Dispose();
    }
}
