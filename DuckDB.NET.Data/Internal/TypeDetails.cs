using System;
using System.Collections.Generic;

namespace DuckDB.NET.Data.Internal;

class TypeDetails
{
    public Dictionary<string, PropertyDetails> Properties { get; set; } = new();
}

record PropertyDetails(Type PropertyType, bool Nullable, Action<object, object> Setter);
