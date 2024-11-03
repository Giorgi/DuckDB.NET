using System;
using System.Collections.Generic;
using System.Data;
using System.Numerics;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal;

internal static class DuckDBTypeMap
{
    private static readonly Dictionary<Type, DbType> ClrToDbTypeMap = new()
    {
        {typeof(bool), DbType.Boolean},
        {typeof(sbyte), DbType.SByte},
        {typeof(short), DbType.Int16},
        {typeof(int), DbType.Int32},
        {typeof(long), DbType.Int64},
        {typeof(float), DbType.Single},
        {typeof(double), DbType.Double},
        {typeof(string), DbType.String},
        {typeof(Guid), DbType.Guid},
        {typeof(decimal), DbType.Currency},
        {typeof(byte), DbType.Byte},
        {typeof(ushort), DbType.UInt16},
        {typeof(uint), DbType.UInt32},
        {typeof(ulong), DbType.UInt64},
        {typeof(BigInteger), DbType.VarNumeric},
        {typeof(byte[]), DbType.Binary},
        {typeof(DateTime), DbType.DateTime},
        {typeof(DuckDBDateOnly), DbType.Date},
        {typeof(DuckDBTimeOnly), DbType.Time},
#if NET6_0_OR_GREATER
        {typeof(DateOnly), DbType.Date},
        {typeof(TimeOnly), DbType.Time},
#endif
    };

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
        { typeof(DateTime), DuckDBType.Timestamp},
        { typeof(TimeSpan), DuckDBType.Interval},
#if NET6_0_OR_GREATER
        { typeof(DateOnly), DuckDBType.Date},
        { typeof(TimeOnly), DuckDBType.Time},
#endif
        { typeof(DateTimeOffset), DuckDBType.TimeTz},
        { typeof(BigInteger), DuckDBType.HugeInt},
        { typeof(string), DuckDBType.Varchar},
        { typeof(decimal), DuckDBType.Decimal},
        { typeof(object), DuckDBType.Any},
    };

    public static DbType GetDbTypeForValue(object? value)
    {
        if (value.IsNull())
        {
            return DbType.Object;
        }

        var type = value!.GetType();

        if (ClrToDbTypeMap.TryGetValue(type, out var dbType))
        {
            return dbType;
        }

        return DbType.Object;
    }

    public static DuckDBLogicalType GetLogicalType<T>() => GetLogicalType(typeof(T));

    public static DuckDBLogicalType GetLogicalType(Type type)
    {
        if (ClrToDuckDBTypeMap.TryGetValue(type, out var duckDBType))
        {
            return NativeMethods.LogicalType.DuckDBCreateLogicalType(duckDBType);
        }

        throw new InvalidOperationException($"Cannot map type {type.FullName} to DuckDBType.");
    }
}