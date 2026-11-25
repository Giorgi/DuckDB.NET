using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Security", "S6640", Scope = "namespaceanddescendants", Target = "~N:DuckDB.NET.Data",
    Justification = "S6640 reports every unsafe block, and the vector readers and writers work on native memory through them.")]
