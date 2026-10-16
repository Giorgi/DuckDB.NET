using System.Reflection;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

internal static class LoadedLibrary
{
    // Shows in the run log which library each job really loaded: check it in the first minutes of a run.
    public static void Print(DuckDBConnection connection)
    {
        var assembly = typeof(DuckDBConnection).Assembly;
        var version = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
        Console.WriteLine($"// DuckDB.NET.Data {version}, DuckDB {connection.ServerVersion}, loaded from {assembly.Location}");
    }
}
