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
    /// Maps a property to the next column in sequence.
    /// </summary>
    /// <typeparam name="TProperty">The property type</typeparam>
    /// <param name="propertyExpression">Expression to select the property</param>
    /// <returns>The current map instance for fluent configuration</returns>
    protected void Map<TProperty>(Expression<Func<T, TProperty>> propertyExpression)
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
            Getter = obj => getter((T)obj),
            MappingType = PropertyMappingType.Property
        };

        propertyMappings.Add(mapping);
    }

    /// <summary>
    /// Adds a default value for the next column.
    /// </summary>
    protected void DefaultValue()
    {
        var mapping = new PropertyMapping
        {
            PropertyName = "<default>",
            PropertyType = typeof(object),
            Getter = _ => null,
            MappingType = PropertyMappingType.Default
        };

        propertyMappings.Add(mapping);
    }

    /// <summary>
    /// Adds a null value for the next column.
    /// </summary>
    protected void NullValue()
    {
        var mapping = new PropertyMapping
        {
            PropertyName = "<null>",
            PropertyType = typeof(object),
            Getter = _ => null,
            MappingType = PropertyMappingType.Null
        };

        propertyMappings.Add(mapping);
    }
}

/// <summary>
/// Represents the type of mapping.
/// </summary>
internal enum PropertyMappingType
{
    Property,
    Default,
    Null
}

/// <summary>
/// Represents a mapping between a property and a column.
/// </summary>
internal class PropertyMapping
{
    public string PropertyName { get; set; } = string.Empty;
    public Type PropertyType { get; set; } = typeof(object);
    public Func<object, object?> Getter { get; set; } = _ => null;
    public PropertyMappingType MappingType { get; set; }
}
