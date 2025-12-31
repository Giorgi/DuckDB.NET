namespace DuckDB.NET.Data.Common;

internal class TypeDetails
{
    public Dictionary<string, PropertyDetails> Properties { get; set; } = new();
}

internal record PropertyDetails(Type PropertyType, bool Nullable, bool NullableValueType, Type? NullableType, Action<object, object> Setter);
