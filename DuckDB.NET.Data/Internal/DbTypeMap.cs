using System;
using System.Collections.Generic;
using System.Data;

namespace DuckDB.NET.Data.Internal;

internal static class DbTypeMap
{
    private static readonly Dictionary<Type, DbType> TypeMap = new()
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
        {typeof(byte), DbType.SByte},
        {typeof(ushort), DbType.UInt16},
        {typeof(uint), DbType.UInt32},
        {typeof(ulong), DbType.UInt64},
        {typeof(DateTime), DbType.DateTime}
    };


    public static DbType GetDbTypeForValue(object value)
    {
        if (value == null)
        {
            throw new ArgumentNullException(nameof(value));
        }
        
        var type = value.GetType();
        
        if (TypeMap.TryGetValue(type, out var dbType))
        {
            return dbType;
        }
        throw new InvalidOperationException($"Values of type {type.FullName} are not supported.");
    }
}
