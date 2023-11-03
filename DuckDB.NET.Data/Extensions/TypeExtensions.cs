using System;

namespace DuckDB.NET.Data.Extensions;

internal static class TypeExtensions
{
    public static bool IsNull(this object? value) => value is null or DBNull;

    public static (bool isNullable, Type type) IsNullable<T>()
    {
        var targetType = typeof(T);

        var isNullable = default(T) is null && targetType.IsValueType;

        return (isNullable, targetType);
    }

    public static bool IsNumericType<T>()
    {
        return Type.GetTypeCode(typeof(T)) switch
        {
            TypeCode.Byte => true,
            TypeCode.SByte => true,
            TypeCode.UInt16 => true,
            TypeCode.UInt32 => true,
            TypeCode.UInt64 => true,
            TypeCode.Int16 => true,
            TypeCode.Int32 => true,
            TypeCode.Int64 => true,
            TypeCode.Decimal => true,
            TypeCode.Double => true,
            TypeCode.Single => true,
            _ => false
        };
    }

    public static bool IsIntegralNumericType<T>()
    {
        return Type.GetTypeCode(typeof(T)) switch
        {
            TypeCode.Byte => true,
            TypeCode.SByte => true,
            TypeCode.UInt16 => true,
            TypeCode.UInt32 => true,
            TypeCode.UInt64 => true,
            TypeCode.Int16 => true,
            TypeCode.Int32 => true,
            TypeCode.Int64 => true,
            _ => false
        };
    }
}