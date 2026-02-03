using DuckDB.NET.Native;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;

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

    private static readonly Dictionary<Type, DuckDBType> ClrToDuckDBTypeMap = new()
    {
        { typeof(bool), DuckDBType.Boolean },
        { typeof(sbyte), DuckDBType.TinyInt },
        { typeof(short), DuckDBType.SmallInt },
        { typeof(int), DuckDBType.Integer },
        { typeof(long), DuckDBType.BigInt },
        { typeof(byte), DuckDBType.UnsignedTinyInt },
        { typeof(ushort), DuckDBType.UnsignedSmallInt },
        { typeof(uint), DuckDBType.UnsignedInteger },
        { typeof(ulong), DuckDBType.UnsignedBigInt },
        { typeof(float), DuckDBType.Float },
        { typeof(double), DuckDBType.Double},
        { typeof(Guid), DuckDBType.Uuid},
        { typeof(DateTime), DuckDBType.Timestamp},
        { typeof(TimeSpan), DuckDBType.Interval},
#if NET6_0_OR_GREATER
        { typeof(DateOnly), DuckDBType.Date},
        { typeof(TimeOnly), DuckDBType.Time},
#endif
        { typeof(DateTimeOffset), DuckDBType.TimestampTz},
        { typeof(BigInteger), DuckDBType.HugeInt},
        { typeof(string), DuckDBType.Varchar},
        { typeof(decimal), DuckDBType.Decimal},
        { typeof(object), DuckDBType.Any},
    };

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

    public static DuckDBLogicalType GetLogicalType<T>() => GetLogicalType(typeof(T));

    public static DuckDBLogicalType GetLogicalType(this Type type)
    {
        if (type == typeof(decimal))
        {
            return NativeMethods.LogicalType.DuckDBCreateDecimalType(38, 18);
        }

        if (ClrToDuckDBTypeMap.TryGetValue(type, out var duckDBType))
        {
            return NativeMethods.LogicalType.DuckDBCreateLogicalType(duckDBType);
        }

        throw new InvalidOperationException($"Cannot map type {type.FullName} to DuckDBType.");
    }

    public static DuckDBType GetDuckDBType(this Type type) => ClrToDuckDBTypeMap.TryGetValue(type, out var duckDBType) ? duckDBType : DuckDBType.Invalid;
}