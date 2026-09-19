using System.ComponentModel;
using System.Globalization;

namespace DuckDB.NET.Data;

/// <summary>
/// Describes a single DuckDB configuration option to <see cref="TypeDescriptor"/> consumers such as property grids
/// and designers. Values are read from and written to the underlying connection string keyword, so a descriptor
/// holds no state of its own.
/// </summary>
internal sealed class DuckDBConfigurationOptionDescriptor : PropertyDescriptor
{
    private readonly string keyword;

    public DuckDBConfigurationOptionDescriptor(string keyword, Type propertyType, Attribute[] attributes)
        : base(keyword, attributes)
    {
        this.keyword = keyword;
        PropertyType = propertyType;
    }

    public override Type ComponentType => typeof(DuckDBConnectionStringBuilder);

    public override Type PropertyType { get; }

    public override bool IsReadOnly => false;

    public override bool CanResetValue(object component) => ((DbConnectionStringBuilder)component).ContainsKey(keyword);

    public override void ResetValue(object component) => ((DbConnectionStringBuilder)component).Remove(keyword);

    public override bool ShouldSerializeValue(object component) => ((DbConnectionStringBuilder)component).ContainsKey(keyword);

    public override object? GetValue(object? component)
    {
        if (component is not DbConnectionStringBuilder builder || !builder.TryGetValue(keyword, out var value))
        {
            return null;
        }

        if (PropertyType.IsInstanceOfType(value))
        {
            return value;
        }

        var text = value.ToString();

        if (string.IsNullOrEmpty(text))
        {
            return PropertyType == typeof(string) ? text : null;
        }

        // A connection string can hold anything the user typed. Returning null for a value that does not fit the
        // option's type keeps the grid empty rather than throwing out of a property enumeration.
        if (PropertyType == typeof(bool))
        {
            return DuckDBConnectionStringBuilder.TryParseBoolean(text!);
        }

        if (PropertyType == typeof(long))
        {
            return long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var number) ? number : null;
        }

        if (PropertyType == typeof(ulong))
        {
            return ulong.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var number) ? number : null;
        }

        if (PropertyType == typeof(double))
        {
            return double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var number) ? number : null;
        }

        return text;
    }

    public override void SetValue(object? component, object? value)
    {
        // Values are stored as invariant text: DuckDB parses the connection string itself, and a culture-formatted
        // number (for example "0,001" for a DOUBLE option) would be rejected by duckdb_set_config.
        ((DbConnectionStringBuilder)component!)[keyword] = value switch
        {
            null => null,
            string text => text,
            bool flag => flag ? "true" : "false",
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString()
        };
    }
}
