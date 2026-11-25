using System.Diagnostics.CodeAnalysis;

[assembly: SuppressMessage("Security", "S6640", Scope = "namespaceanddescendants", Target = "~N:DuckDB.NET.Native",
    Justification = "S6640 reports every unsafe block, and native interop cannot work without them.")]
