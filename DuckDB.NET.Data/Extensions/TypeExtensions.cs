using System;
using System.Collections.Generic;
using System.Numerics;

namespace DuckDB.NET.Data.Extensions;

internal static class TypeExtensions
{
    private static readonly HashSet<Type> FloatingNumericTypes = new()
    {
        typeof(decimal), typeof(float), typeof(double)
    };

    private static readonly HashSet<Type> IntegralNumericTypes = new()
    {
        typeof(byte), typeof(sbyte),
        typeof(short), typeof(ushort), 
        typeof(int), typeof(uint), 
        typeof(long),typeof(ulong),
        typeof(BigInteger)
    };

    public static bool IsNull(this object? value) => value is null or DBNull;

    public static (bool isNullable, Type type) IsNullable<T>()
    {
        var targetType = typeof(T);

        var isNullable = default(T) is null && targetType.IsValueType;

        return (isNullable, targetType);
    }

    public static bool IsFloatingNumericType<T>()
    {
        return FloatingNumericTypes.Contains(typeof(T));
    }

    public static bool IsIntegralNumericType<T>()
    {
        return IntegralNumericTypes.Contains(typeof(T));
    }
}