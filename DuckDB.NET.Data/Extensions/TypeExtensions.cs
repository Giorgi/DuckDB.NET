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
}