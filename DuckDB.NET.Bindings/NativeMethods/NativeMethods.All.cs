using System.Diagnostics.CodeAnalysis;

namespace DuckDB.NET.Native;

[SuppressMessage("Design", "S4200", Justification = "The P/Invoke declarations are the public API of the bindings package.")]
public static partial class NativeMethods
{
    private const string DuckDbLibrary = "duckdb";

    //Grouped according to https://duckdb.org/docs/archive/0.8.1/api/c/api
}