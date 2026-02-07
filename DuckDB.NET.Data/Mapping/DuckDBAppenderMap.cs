using DuckDB.NET.Native;
using System;
using System.Collections.Generic;
using System.Numerics;

namespace DuckDB.NET.Data.Mapping;

/// <summary>
/// Base class for defining mappings between .NET classes and DuckDB table columns for appender operations.
/// </summary>
/// <typeparam name="T">The type to map</typeparam>
public abstract class DuckDBAppenderMap<T>
{
    /// <summary>
    /// Gets the property mappings defined for this class map.
    /// </summary>
    internal List<IPropertyMapping<T>> PropertyMappings { get; } = new(8);

    /// <summary>
    /// Maps a property to the next column in sequence.
    /// </summary>
    /// <typeparam name="TProperty">The property type</typeparam>
    /// <param name="getter">Function to get the property value</param>
    protected void Map<TProperty>(Func<T, TProperty> getter)
    {
        var mapping = new PropertyMapping<T, TProperty>
        {
            PropertyType = typeof(TProperty),
            Getter = getter,
            MappingType = PropertyMappingType.Property
        };

        PropertyMappings.Add(mapping);
    }

    /// <summary>
    /// Adds a default value for the next column.
    /// </summary>
    protected void DefaultValue()
    {
        var mapping = new DefaultValueMapping<T>
        {
            PropertyType = typeof(object),
            MappingType = PropertyMappingType.Default
        };

        PropertyMappings.Add(mapping);
    }

    /// <summary>
    /// Adds a null value for the next column.
    /// </summary>
    protected void NullValue()
    {
        var mapping = new NullValueMapping<T>
        {
            PropertyType = typeof(object),
            MappingType = PropertyMappingType.Null
        };

        PropertyMappings.Add(mapping);
    }
}

internal enum PropertyMappingType
{
    Property,
    Default,
    Null
}

internal interface IPropertyMapping<T>
{
    Type PropertyType { get; }
    PropertyMappingType MappingType { get; }
    IDuckDBAppenderRow AppendToRow(IDuckDBAppenderRow row, T record);
}

internal sealed class PropertyMapping<T, TProperty> : IPropertyMapping<T>
{
    public Type PropertyType { get; set; } = typeof(object);
    public Func<T, TProperty> Getter { get; set; } = _ => default!;
    public PropertyMappingType MappingType { get; set; }

    public IDuckDBAppenderRow AppendToRow(IDuckDBAppenderRow row, T record)
    {
        var value = Getter(record);

        if (value is null)
        {
            return row.AppendNullValue();
        }

        return value switch
        {
            // Reference types
            string v => row.AppendValue(v),

            // Value types
            bool v => row.AppendValue(v),
            sbyte v => row.AppendValue(v),
            short v => row.AppendValue(v),
            int v => row.AppendValue(v),
            long v => row.AppendValue(v),
            byte v => row.AppendValue(v),
            ushort v => row.AppendValue(v),
            uint v => row.AppendValue(v),
            ulong v => row.AppendValue(v),
            float v => row.AppendValue(v),
            double v => row.AppendValue(v),
            decimal v => row.AppendValue(v),
            DateTime v => row.AppendValue(v),
            DateTimeOffset v => row.AppendValue(v),
            TimeSpan v => row.AppendValue(v),
            Guid v => row.AppendValue(v),
            BigInteger v => row.AppendValue(v),
            DuckDBDateOnly v => row.AppendValue(v),
            DuckDBTimeOnly v => row.AppendValue(v),
            DateOnly v => row.AppendValue(v),
            TimeOnly v => row.AppendValue(v),

            _ => throw new NotSupportedException($"Type {typeof(TProperty).Name} is not supported for appending")
        };
    }
}

internal sealed class DefaultValueMapping<T> : IPropertyMapping<T>
{
    public Type PropertyType { get; set; } = typeof(object);
    public PropertyMappingType MappingType { get; set; }

    public IDuckDBAppenderRow AppendToRow(IDuckDBAppenderRow row, T record)
    {
        return row.AppendDefault();
    }
}

internal sealed class NullValueMapping<T> : IPropertyMapping<T>
{
    public Type PropertyType { get; set; } = typeof(object);
    public PropertyMappingType MappingType { get; set; }

    public IDuckDBAppenderRow AppendToRow(IDuckDBAppenderRow row, T record)
    {
        return row.AppendNullValue();
    }
}
