using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Reflection;

namespace DuckDB.NET.Data.Extensions;

internal static class TypeExtensions
{
    private static readonly HashSet<Type> FloatingNumericTypes = [typeof(decimal), typeof(float), typeof(double)];

    private static readonly HashSet<Type> IntegralNumericTypes =
    [
        typeof(byte), typeof(sbyte),
        typeof(short), typeof(ushort),
        typeof(int), typeof(uint),
        typeof(long), typeof(ulong),
        typeof(BigInteger)
    ];

    public static bool IsNull([NotNullWhen(false)] this object? value) => value is null or DBNull;

    public static (bool isNullableValueType, Type type) IsNullableValueType<T>()
    {
        var targetType = typeof(T);

        var isNullableValueType = default(T) is null && targetType.IsValueType;

        return (isNullableValueType, targetType);
    }

    public static bool IsFloatingNumericType<T>()
    {
        return FloatingNumericTypes.Contains(typeof(T));
    }

    public static bool IsIntegralNumericType<T>()
    {
        return IntegralNumericTypes.Contains(typeof(T));
    }

    public static bool IsNumeric(this Type type)
    {
        return IntegralNumericTypes.Contains(type) || FloatingNumericTypes.Contains(type);
    }

    public static bool AllowsNullValue(this Type type, out bool isNullableValueType, out Type? underlyingType)
    {
        underlyingType = Nullable.GetUnderlyingType(type);
        isNullableValueType = underlyingType != null;

        var isNullable = isNullableValueType || !type.IsValueType;

        return isNullable;
    }
}