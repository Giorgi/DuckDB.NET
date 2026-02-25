namespace DuckDB.NET.Data;

[AttributeUsage(AttributeTargets.Parameter)]
public class NamedAttribute : Attribute
{
    public string? Name { get; }
    public NamedAttribute() { }
    public NamedAttribute(string name) => Name = name;
}
