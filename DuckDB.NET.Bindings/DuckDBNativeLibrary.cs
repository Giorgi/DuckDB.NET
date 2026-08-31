using System.IO;
using System.Reflection;

namespace DuckDB.NET.Native;

/// <summary>
/// Resolves the native DuckDB library from the runtimes folder beside this assembly.
/// </summary>
/// <remarks>
/// A NuGet package writes a runtimeTargets entry into deps.json and the host probes
/// runtimes/{rid}/native on its own. A project reference produces no such entry, so without this
/// resolver every project that consumes DuckDB.NET by reference has to load the library itself.
/// </remarks>
internal static class DuckDBNativeLibrary
{
    // CA2255 warns against ModuleInitializer in a library. Registering a DllImport resolver is the
    // case the rule exempts: the resolver has to be in place before the first P/Invoke, and those
    // live on nested types whose static constructors the outer type cannot hook.
#pragma warning disable CA2255
    [ModuleInitializer]
#pragma warning restore CA2255
    internal static void Register() =>
        NativeLibrary.SetDllImportResolver(typeof(DuckDBNativeLibrary).Assembly, Resolve);

    private static IntPtr Resolve(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
    {
        if (libraryName != NativeMethods.DuckDbLibrary || GetRuntimeIdentifier() is not { } rid)
        {
            return IntPtr.Zero;
        }

        // The four-argument overload applies the platform naming convention, so "duckdb" matches
        // libduckdb.dylib, libduckdb.so and duckdb.dll. The two-argument one needs an exact path.
        // Passing a relative path keeps this working under single-file publish, where
        // Assembly.Location is empty.
        foreach (var candidate in (string[])["duckdb", "libduckdb"])
        {
            var path = Path.Combine("runtimes", rid, "native", candidate);

            if (NativeLibrary.TryLoad(path, assembly, DllImportSearchPath.AssemblyDirectory, out var handle))
            {
                return handle;
            }
        }

        // Zero hands the name back to the default probing, so a library already on the system or
        // resolved from a NuGet package still loads.
        return IntPtr.Zero;
    }

    /// <remarks>
    /// These identifiers name the folders the build stages, not the full RID graph. DuckDB ships one
    /// universal macOS binary, so that folder is "osx" rather than osx-x64 and osx-arm64.
    /// </remarks>
    private static string? GetRuntimeIdentifier()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            return Environment.Is64BitProcess ? "win-x64" : "win-x86";
        }

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            return RuntimeInformation.ProcessArchitecture switch
            {
                Architecture.X64 => "linux-x64",
                Architecture.Arm64 => "linux-arm64",
                _ => null,
            };
        }

        return RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? "osx" : null;
    }
}
