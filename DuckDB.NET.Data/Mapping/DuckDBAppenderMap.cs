using System.Runtime.CompilerServices;

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
    void AppendToRow(IDuckDBAppenderRow row, T record);
}

internal sealed class PropertyMapping<T, TProperty> : IPropertyMapping<T>
{
    public Type PropertyType { get; set; } = typeof(object);
    public Func<T, TProperty> Getter { get; set; } = _ => default!;
    public PropertyMappingType MappingType { get; set; }

    public void AppendToRow(IDuckDBAppenderRow row, T record)
    {
        var value = Getter(record);

        // Both conditions are constants for the JIT. A nullable value type must not reach the patterns below:
        // testing it against a type boxes it, once for every pattern that is tried.
        if (typeof(TProperty).IsValueType && default(TProperty) is null)
        {
            AppendNullable(row, ref value);
            return;
        }

        if (value is null)
        {
            row.AppendNullValue();
            return;
        }

        _ = value switch
        {
            // Reference types
            string v => row.AppendValue(v),
            byte[] v => row.AppendValue(v),

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

    // Every AppendValue overload takes a nullable, so the value is passed on as it is. typeof(TProperty) is a
    // constant for the JIT, which keeps the one matching line.
    private static void AppendNullable(IDuckDBAppenderRow row, ref TProperty value)
    {
        if (typeof(TProperty) == typeof(bool?)) row.AppendValue(Unsafe.As<TProperty, bool?>(ref value));
        else if (typeof(TProperty) == typeof(sbyte?)) row.AppendValue(Unsafe.As<TProperty, sbyte?>(ref value));
        else if (typeof(TProperty) == typeof(short?)) row.AppendValue(Unsafe.As<TProperty, short?>(ref value));
        else if (typeof(TProperty) == typeof(int?)) row.AppendValue(Unsafe.As<TProperty, int?>(ref value));
        else if (typeof(TProperty) == typeof(long?)) row.AppendValue(Unsafe.As<TProperty, long?>(ref value));
        else if (typeof(TProperty) == typeof(byte?)) row.AppendValue(Unsafe.As<TProperty, byte?>(ref value));
        else if (typeof(TProperty) == typeof(ushort?)) row.AppendValue(Unsafe.As<TProperty, ushort?>(ref value));
        else if (typeof(TProperty) == typeof(uint?)) row.AppendValue(Unsafe.As<TProperty, uint?>(ref value));
        else if (typeof(TProperty) == typeof(ulong?)) row.AppendValue(Unsafe.As<TProperty, ulong?>(ref value));
        else if (typeof(TProperty) == typeof(float?)) row.AppendValue(Unsafe.As<TProperty, float?>(ref value));
        else if (typeof(TProperty) == typeof(double?)) row.AppendValue(Unsafe.As<TProperty, double?>(ref value));
        else if (typeof(TProperty) == typeof(decimal?)) row.AppendValue(Unsafe.As<TProperty, decimal?>(ref value));
        else if (typeof(TProperty) == typeof(DateTime?)) row.AppendValue(Unsafe.As<TProperty, DateTime?>(ref value));
        else if (typeof(TProperty) == typeof(DateTimeOffset?)) row.AppendValue(Unsafe.As<TProperty, DateTimeOffset?>(ref value));
        else if (typeof(TProperty) == typeof(TimeSpan?)) row.AppendValue(Unsafe.As<TProperty, TimeSpan?>(ref value));
        else if (typeof(TProperty) == typeof(Guid?)) row.AppendValue(Unsafe.As<TProperty, Guid?>(ref value));
        else if (typeof(TProperty) == typeof(BigInteger?)) row.AppendValue(Unsafe.As<TProperty, BigInteger?>(ref value));
        else if (typeof(TProperty) == typeof(DuckDBDateOnly?)) row.AppendValue(Unsafe.As<TProperty, DuckDBDateOnly?>(ref value));
        else if (typeof(TProperty) == typeof(DuckDBTimeOnly?)) row.AppendValue(Unsafe.As<TProperty, DuckDBTimeOnly?>(ref value));
        else if (typeof(TProperty) == typeof(DateOnly?)) row.AppendValue(Unsafe.As<TProperty, DateOnly?>(ref value));
        else if (typeof(TProperty) == typeof(TimeOnly?)) row.AppendValue(Unsafe.As<TProperty, TimeOnly?>(ref value));
        else throw new NotSupportedException($"Type {typeof(TProperty).Name} is not supported for appending");
    }
}

internal sealed class DefaultValueMapping<T> : IPropertyMapping<T>
{
    public Type PropertyType { get; set; } = typeof(object);
    public PropertyMappingType MappingType { get; set; }

    public void AppendToRow(IDuckDBAppenderRow row, T record)
    {
        row.AppendDefault();
    }
}

internal sealed class NullValueMapping<T> : IPropertyMapping<T>
{
    public Type PropertyType { get; set; } = typeof(object);
    public PropertyMappingType MappingType { get; set; }

    public void AppendToRow(IDuckDBAppenderRow row, T record)
    {
        row.AppendNullValue();
    }
}
