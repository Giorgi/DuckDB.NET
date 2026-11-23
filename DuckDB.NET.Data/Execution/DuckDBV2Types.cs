using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Execution;

/// <summary>Creates V2 logical types: from a type id, from SQL text, and for a CLR type.</summary>
internal static unsafe class DuckDBV2Types
{
    // V1's numbers back to V2's ids, for the types a CLR type maps to.
    private static readonly Dictionary<DuckDBType, DUCKDB_V2_LOGICAL_TYPE_ID> Ids = BuildIds();

    public static DuckDBV2LogicalType FromId(IntPtr factory, DUCKDB_V2_LOGICAL_TYPE_ID id)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_logical_type* type;
        NativeMethodsV2.duckdb_v2_factory_create_type_from_id((_duckdb_v2_factory*)factory, id, null, null, null, 0, &type, &error).ThrowIfFailed(ref error);
        return new DuckDBV2LogicalType(type);
    }

    /// <summary>The logical type of a V1 type id, for the API that takes <see cref="DuckDBType"/>.</summary>
    public static DuckDBV2LogicalType FromDuckDBType(IntPtr factory, DuckDBType type) =>
        Ids.TryGetValue(type, out var id) ? FromId(factory, id) : throw new InvalidOperationException($"Cannot map {type} to a logical type.");

    public static DuckDBV2LogicalType FromText(IntPtr factory, string text)
    {
        using var buffer = DuckDBV2Utf8.FromManaged(text);

        fixed (byte* pointer = buffer.Bytes)
        {
            _duckdb_v2_error_info* error = null;
            _duckdb_v2_logical_type* type;
            NativeMethodsV2.duckdb_v2_factory_create_type_from_text((_duckdb_v2_factory*)factory, (sbyte*)pointer, (ulong)buffer.Length, &type, &error)
                           .ThrowIfFailed(ref error);
            return new DuckDBV2LogicalType(type);
        }
    }

    public static string ToText(_duckdb_v2_logical_type* type)
    {
        _duckdb_v2_error_info* error = null;
        ulong length;
        NativeMethodsV2.duckdb_v2_logical_type_to_text(type, null, 0, &length, &error).ThrowIfFailed(ref error);

        var text = new sbyte[length + 1];
        fixed (sbyte* pointer = text)
        {
            NativeMethodsV2.duckdb_v2_logical_type_to_text(type, pointer, (ulong)text.Length, &length, &error).ThrowIfFailed(ref error);
            return DuckDBV2Utf8.ToManaged(pointer, length);
        }
    }

    /// <summary>
    /// The logical type for a CLR type, as <see cref="TypeExtensions.GetLogicalType(Type)"/> gives it with V1: a
    /// <c>decimal</c> is DECIMAL(38,18), a generic list a LIST of its element's type.
    /// </summary>
    public static DuckDBV2LogicalType ForClrType(IntPtr factory, Type type)
    {
        type = type.UnderlyingTypeOrSelf();

        if (type == typeof(decimal))
        {
            return FromText(factory, "DECIMAL(38,18)");
        }

        if (type.IsAssignableTo(typeof(IList)) && type.IsGenericType)
        {
            using var itemType = ForClrType(factory, type.GetGenericArguments()[0]);
            using var lease = itemType.Acquire();
            return FromText(factory, ToText(lease.Pointer) + "[]");
        }

        if (Ids.TryGetValue(type.GetDuckDBType(), out var id))
        {
            return FromId(factory, id);
        }

        throw new InvalidOperationException($"Cannot map type {type.FullName} to DuckDBType.");
    }

    private static Dictionary<DuckDBType, DUCKDB_V2_LOGICAL_TYPE_ID> BuildIds()
    {
        var ids = new Dictionary<DuckDBType, DUCKDB_V2_LOGICAL_TYPE_ID>();

        foreach (var id in Enum.GetValues<DUCKDB_V2_LOGICAL_TYPE_ID>())
        {
            var duckDBType = id.ToDuckDBType();

            if (duckDBType != DuckDBType.Invalid)
            {
                ids.TryAdd(duckDBType, id);
            }
        }

        return ids;
    }
}
