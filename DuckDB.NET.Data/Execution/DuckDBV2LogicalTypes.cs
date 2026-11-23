using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Execution;

/// <summary>
/// Reads the parameters of a V2 logical type. V2 has no getter per kind (V1's duckdb_decimal_width,
/// duckdb_enum_dictionary_value): a type's parameters are values, DECIMAL's width and scale as UINTEGER and an enum's
/// entries as VARCHAR, one parameter per entry.
/// </summary>
internal static unsafe class DuckDBV2LogicalTypes
{
    public static DUCKDB_V2_LOGICAL_TYPE_ID GetId(_duckdb_v2_logical_type* type)
    {
        _duckdb_v2_error_info* error = null;
        DUCKDB_V2_LOGICAL_TYPE_ID id;
        NativeMethodsV2.duckdb_v2_logical_type_get_id(type, &id, &error).ThrowIfFailed(ref error);
        return id;
    }

    public static ulong GetParamCount(_duckdb_v2_logical_type* type)
    {
        _duckdb_v2_error_info* error = null;
        ulong count;
        NativeMethodsV2.duckdb_v2_logical_type_get_param_count(type, &count, &error).ThrowIfFailed(ref error);
        return count;
    }

    public static uint GetUIntParam(_duckdb_v2_logical_type* type, ulong index)
    {
        var value = GetParam(type, index);

        try
        {
            _duckdb_v2_error_info* error = null;
            uint result;
            NativeMethodsV2.duckdb_v2_value_get_uint(value, &result, &error).ThrowIfFailed(ref error);
            return result;
        }
        finally
        {
            NativeMethodsV2.duckdb_v2_value_destroy(&value);
        }
    }

    public static string GetStringParam(_duckdb_v2_logical_type* type, ulong index)
    {
        var value = GetParam(type, index);

        try
        {
            _duckdb_v2_error_info* error = null;
            sbyte* text;
            ulong length;
            NativeMethodsV2.duckdb_v2_value_get_varchar(value, &text, &length, &error).ThrowIfFailed(ref error);

            // Borrowed from the value, so copied before the value goes.
            return DuckDBV2Utf8.ToManaged(text, length);
        }
        finally
        {
            NativeMethodsV2.duckdb_v2_value_destroy(&value);
        }
    }

    public static long GetBigIntParam(_duckdb_v2_logical_type* type, ulong index)
    {
        var value = GetParam(type, index);

        try
        {
            _duckdb_v2_error_info* error = null;
            long result;
            NativeMethodsV2.duckdb_v2_value_get_bigint(value, &result, &error).ThrowIfFailed(ref error);
            return result;
        }
        finally
        {
            NativeMethodsV2.duckdb_v2_value_destroy(&value);
        }
    }

    /// <summary>
    /// A parameter that is a type, such as a LIST's element type or a STRUCT's field, with its name: the field's
    /// name for a STRUCT, empty for a positional parameter. The type is owned by the caller.
    /// </summary>
    public static DuckDBV2LogicalType GetTypeParam(_duckdb_v2_logical_type* type, ulong index, out string name)
    {
        var value = GetParam(type, index, out name);

        try
        {
            _duckdb_v2_error_info* error = null;
            _duckdb_v2_logical_type* childType;
            NativeMethodsV2.duckdb_v2_value_get_type(value, &childType, &error).ThrowIfFailed(ref error);
            return new DuckDBV2LogicalType(childType);
        }
        finally
        {
            NativeMethodsV2.duckdb_v2_value_destroy(&value);
        }
    }

    public static DuckDBV2LogicalType Copy(_duckdb_v2_logical_type* type)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_logical_type* copy;
        NativeMethodsV2.duckdb_v2_logical_type_copy(type, &copy, &error).ThrowIfFailed(ref error);
        return new DuckDBV2LogicalType(copy);
    }

    private static _duckdb_v2_value* GetParam(_duckdb_v2_logical_type* type, ulong index) => GetParam(type, index, out _);

    private static _duckdb_v2_value* GetParam(_duckdb_v2_logical_type* type, ulong index, out string name)
    {
        _duckdb_v2_error_info* error = null;
        sbyte* namePointer;
        ulong nameLength;
        _duckdb_v2_value* value;
        NativeMethodsV2.duckdb_v2_logical_type_get_param(type, index, &namePointer, &nameLength, &value, &error).ThrowIfFailed(ref error);

        // Borrowed from the type, so copied now.
        name = DuckDBV2Utf8.ToManaged(namePointer, nameLength);
        return value;
    }
}
