using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.DataChunk.Reader;

internal static unsafe class DuckDBV2Vectors
{
    /// <summary>
    /// The data and validity of a vector, flattened first when it is a constant, dictionary or other non-flat vector,
    /// so that row <c>i</c> is element <c>i</c>. A flat vector's view has no selection.
    /// </summary>
    public static duckdb_v2_vector_view ReadFlat(_duckdb_v2_vector* vector)
    {
        _duckdb_v2_error_info* error = null;

        DUCKDB_V2_VECTOR_TYPE vectorType;
        NativeMethodsV2.duckdb_v2_vector_get_vector_type(vector, &vectorType, &error).ThrowIfFailed(ref error);

        if (vectorType != DUCKDB_V2_VECTOR_TYPE.FLAT)
        {
            NativeMethodsV2.duckdb_v2_vector_flatten(vector, &error).ThrowIfFailed(ref error);
        }

        duckdb_v2_vector_view view;
        NativeMethodsV2.duckdb_v2_vector_get_view(vector, &view, &error).ThrowIfFailed(ref error);

        if (view.sel != null)
        {
            throw new InvalidOperationException("A flattened vector still has a selection vector.");
        }

        return view;
    }

    public static _duckdb_v2_vector* GetVector(DuckDBV2DataChunk chunk, int index)
    {
        using var lease = chunk.Acquire();
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_vector* vector;
        NativeMethodsV2.duckdb_v2_data_chunk_get_vector(lease.Pointer, (ulong)index, &vector, &error).ThrowIfFailed(ref error);

        // Borrowed from the chunk: valid while the chunk is.
        return vector;
    }
}
