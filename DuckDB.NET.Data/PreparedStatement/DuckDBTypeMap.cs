using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;
using System;
using System.Collections.Generic;
using System.Data;
using System.Numerics;

namespace DuckDB.NET.Data.PreparedStatement;

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
        {typeof(DateTimeOffset), DbType.DateTimeOffset},
        {typeof(DuckDBDateOnly), DbType.Date},
        {typeof(DuckDBTimeOnly), DbType.Time},
#if NET6_0_OR_GREATER
        {typeof(DateOnly), DbType.Date},
        {typeof(TimeOnly), DbType.Time},
#endif
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
}