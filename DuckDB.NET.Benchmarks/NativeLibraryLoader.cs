using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Benchmarks;

/// <summary>
/// Loads the platform-native DuckDB library for the benchmark process.
/// </summary>
internal static class NativeLibraryLoader
{
    [ModuleInitializer]
    public static void Init()
    {
        if (GetRid() is not { } rid)
        {
            return;
        }

        _ = NativeLibrary.TryLoad(Path.Join("runtimes", rid, "native", "duckdb"), Assembly.GetExecutingAssembly(), DllImportSearchPath.AssemblyDirectory, out _) ||
            NativeLibrary.TryLoad(Path.Join("runtimes", rid, "native", "libduckdb"), Assembly.GetExecutingAssembly(), DllImportSearchPath.AssemblyDirectory, out _);
    }

    private static string? GetRid()
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

        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
        {
            return "osx";
        }

        return null;
    }
}
