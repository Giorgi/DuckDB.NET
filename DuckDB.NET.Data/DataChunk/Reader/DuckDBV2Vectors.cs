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

    /// <summary>
    /// A child of a nested vector, borrowed from its chunk: [0] the elements of a LIST or ARRAY, [0] the entries of a
    /// MAP (a STRUCT of key and value), [i] field i of a STRUCT. Flatten the parent first: a child taken from a
    /// dictionary or constant vector is not indexed by the parent's rows.
    /// </summary>
    public static _duckdb_v2_vector* GetChild(_duckdb_v2_vector* vector, int index)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_vector* child;
        NativeMethodsV2.duckdb_v2_vector_get_child(vector, (ulong)index, &child, &error).ThrowIfFailed(ref error);
        return child;
    }

    /// <summary>The data of a FLAT or CONSTANT vector, for writing. Valid until the vector is reallocated.</summary>
    public static void* GetMutableData(_duckdb_v2_vector* vector)
    {
        _duckdb_v2_error_info* error = null;
        void* data;
        NativeMethodsV2.duckdb_v2_vector_get_data_mutable(vector, &data, &error).ThrowIfFailed(ref error);
        return data;
    }

    /// <summary>The validity mask of a FLAT vector, for writing; allocated, all rows valid, on first use.</summary>
    public static ulong* GetMutableValidity(_duckdb_v2_vector* vector)
    {
        _duckdb_v2_error_info* error = null;
        ulong* validity;
        NativeMethodsV2.duckdb_v2_vector_flat_get_validity_mutable(vector, &validity, &error).ThrowIfFailed(ref error);
        return validity;
    }

    /// <summary>
    /// The arena that holds the bytes of a VARCHAR or BLOB vector's long values, allocated on first use. Borrowed, and
    /// valid until the vector is flattened, reallocated or destroyed.
    /// </summary>
    public static _duckdb_v2_arena* GetArena(_duckdb_v2_vector* vector)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_arena* arena;
        NativeMethodsV2.duckdb_v2_vector_get_arena(vector, &arena, &error).ThrowIfFailed(ref error);
        return arena;
    }

    /// <summary>Sets how many elements the vector holds, reserving room for them. V2 sizes each vector; there is no chunk size to set.</summary>
    public static void SetSize(_duckdb_v2_vector* vector, ulong size)
    {
        _duckdb_v2_error_info* error = null;
        NativeMethodsV2.duckdb_v2_vector_set_size(vector, size, &error).ThrowIfFailed(ref error);
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
