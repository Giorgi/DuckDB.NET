using System;
using System.Collections.Generic;

namespace DuckDB.NET.Data.Internal;

internal class TypeDetails
{
    public Dictionary<string, PropertyDetails> Properties { get; set; } = new();
}

internal record PropertyDetails(Type PropertyType, bool Nullable, Action<object, object> Setter);
