namespace DuckDB.NET.Data;

[AttributeUsage(AttributeTargets.Parameter)]
public class NamedAttribute : Attribute
{
    public string? Name { get; }

    private object? defaultValue;

    /// <summary>
    /// The value the parameter takes when a call leaves it out. Without one the parameter is required, and DuckDB
    /// refuses a call without it. <c>null</c> makes it optional and NULL. A number, <c>bool</c>, enum or string of the
    /// parameter's type is the value itself; for other types (<c>decimal</c>, dates, times, <see cref="Guid"/>, lists)
    /// give it as text, which DuckDB casts to the parameter's type, e.g. <c>"2024-01-01"</c> or <c>"[1, 2, 3]"</c>. The
    /// default is checked when the function is registered.
    /// </summary>
    public object? Default
    {
        get => defaultValue;
        set
        {
            defaultValue = value;
            HasDefault = true;
        }
    }

    /// <summary>Whether <see cref="Default"/> was set, to <c>null</c> or a value.</summary>
    public bool HasDefault { get; private set; }
    public NamedAttribute() { }
    public NamedAttribute(string name) => Name = name;
}
