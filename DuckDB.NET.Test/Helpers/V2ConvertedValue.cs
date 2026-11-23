using DuckDB.NET.Data.Execution;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Data.PreparedStatement;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Test.Helpers;

// Converts a value as a parameter of the given type would be, through the connection's factory, and reports the type
// of the value the converter created.
internal static unsafe class V2ConvertedValue
{
    public static (DuckDBType Type, byte Width, byte Scale) Convert(DuckDBConnection connection, object value, Func<IntPtr, DuckDBV2LogicalType> parameterType,
                                                                   DuckDBType parameterDuckDBType, DbType dbType)
    {
        using var lease = connection.V2Connection.Acquire();

        _duckdb_v2_error_info* error = null;
        _duckdb_v2_factory* factory;
        NativeMethodsV2.duckdb_v2_connection_get_factory(lease.Pointer, &factory, &error).ThrowIfFailed(ref error);

        using var type = parameterType((IntPtr)factory);
        using var typeLease = type.Acquire();
        using var converted = value.ToDuckDBValue((IntPtr)factory, (IntPtr)typeLease.Pointer, parameterDuckDBType, dbType);
        using var valueLease = converted.Acquire();

        _duckdb_v2_logical_type* valueType;
        NativeMethodsV2.duckdb_v2_value_get_logical_type(valueLease.Pointer, &valueType, &error).ThrowIfFailed(ref error);

        try
        {
            var duckDBType = DuckDBV2LogicalTypes.GetId(valueType).ToDuckDBType();
            return duckDBType == DuckDBType.Decimal
                ? (duckDBType, (byte)DuckDBV2LogicalTypes.GetUIntParam(valueType, 0), (byte)DuckDBV2LogicalTypes.GetUIntParam(valueType, 1))
                : (duckDBType, (byte)0, (byte)0);
        }
        finally
        {
            NativeMethodsV2.duckdb_v2_logical_type_destroy(&valueType);
        }
    }
}
