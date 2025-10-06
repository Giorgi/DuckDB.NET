using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Mapping;

/// <summary>
/// Base class for defining mappings between .NET classes and DuckDB table columns.
/// </summary>
/// <typeparam name="T">The type to map</typeparam>
public abstract class DuckDBClassMap<T>
{
    private readonly List<PropertyMapping> propertyMappings = new();

    /// <summary>
    /// Gets the property mappings defined for this class map.
    /// </summary>
    internal IReadOnlyList<PropertyMapping> PropertyMappings => propertyMappings;

    /// <summary>
    /// Maps a property to a column with type validation.
    /// </summary>
    /// <typeparam name="TProperty">The property type</typeparam>
    /// <param name="propertyExpression">Expression to select the property</param>
    /// <param name="columnType">The expected DuckDB column type</param>
    /// <returns>The current map instance for fluent configuration</returns>
    protected PropertyMappingBuilder<T, TProperty> Map<TProperty>(
        Expression<Func<T, TProperty>> propertyExpression,
        DuckDBType columnType)
    {
        if (propertyExpression.Body is not MemberExpression memberExpression)
        {
            throw new ArgumentException("Expression must be a member expression", nameof(propertyExpression));
        }

        var propertyName = memberExpression.Member.Name;
        var propertyType = typeof(TProperty);
        var getter = propertyExpression.Compile();

        var mapping = new PropertyMapping
        {
            PropertyName = propertyName,
            PropertyType = propertyType,
            ColumnType = columnType,
            Getter = obj => getter((T)obj)
        };

        propertyMappings.Add(mapping);

        return new PropertyMappingBuilder<T, TProperty>(mapping);
    }

    /// <summary>
    /// Maps a property to a column, automatically inferring the DuckDB type.
    /// </summary>
    /// <typeparam name="TProperty">The property type</typeparam>
    /// <param name="propertyExpression">Expression to select the property</param>
    /// <returns>The current map instance for fluent configuration</returns>
    protected PropertyMappingBuilder<T, TProperty> Map<TProperty>(
        Expression<Func<T, TProperty>> propertyExpression)
    {
        var columnType = InferDuckDBType(typeof(TProperty));
        return Map(propertyExpression, columnType);
    }

    private static DuckDBType InferDuckDBType(Type type)
    {
        // Handle nullable types
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;

        return underlyingType switch
        {
            Type t when t == typeof(bool) => DuckDBType.Boolean,
            Type t when t == typeof(sbyte) => DuckDBType.TinyInt,
            Type t when t == typeof(short) => DuckDBType.SmallInt,
            Type t when t == typeof(int) => DuckDBType.Integer,
            Type t when t == typeof(long) => DuckDBType.BigInt,
            Type t when t == typeof(byte) => DuckDBType.UnsignedTinyInt,
            Type t when t == typeof(ushort) => DuckDBType.UnsignedSmallInt,
            Type t when t == typeof(uint) => DuckDBType.UnsignedInteger,
            Type t when t == typeof(ulong) => DuckDBType.UnsignedBigInt,
            Type t when t == typeof(float) => DuckDBType.Float,
            Type t when t == typeof(double) => DuckDBType.Double,
            Type t when t == typeof(decimal) => DuckDBType.Decimal,
            Type t when t == typeof(string) => DuckDBType.Varchar,
            Type t when t == typeof(DateTime) => DuckDBType.Timestamp,
            Type t when t == typeof(DateTimeOffset) => DuckDBType.TimestampTz,
            Type t when t == typeof(TimeSpan) => DuckDBType.Interval,
            Type t when t == typeof(Guid) => DuckDBType.Uuid,
#if NET6_0_OR_GREATER
            Type t when t == typeof(DateOnly) => DuckDBType.Date,
            Type t when t == typeof(TimeOnly) => DuckDBType.Time,
#endif
            _ => throw new NotSupportedException($"Type {type.Name} is not supported for automatic mapping")
        };
    }
}

/// <summary>
/// Represents a mapping between a property and a column.
/// </summary>
internal class PropertyMapping
{
    public string PropertyName { get; set; } = string.Empty;
    public Type PropertyType { get; set; } = typeof(object);
    public DuckDBType ColumnType { get; set; }
    public Func<object, object?> Getter { get; set; } = _ => null;
    public int? ColumnIndex { get; set; }
}

/// <summary>
/// Builder for configuring property mappings.
/// </summary>
/// <typeparam name="T">The mapped class type</typeparam>
/// <typeparam name="TProperty">The property type</typeparam>
public class PropertyMappingBuilder<T, TProperty>
{
    private readonly PropertyMapping mapping;

    internal PropertyMappingBuilder(PropertyMapping mapping)
    {
        this.mapping = mapping;
    }

    /// <summary>
    /// Specifies the column index for this property mapping.
    /// </summary>
    /// <param name="columnIndex">The zero-based column index</param>
    /// <returns>The builder for fluent configuration</returns>
    public PropertyMappingBuilder<T, TProperty> ToColumn(int columnIndex)
    {
        mapping.ColumnIndex = columnIndex;
        return this;
    }
}
