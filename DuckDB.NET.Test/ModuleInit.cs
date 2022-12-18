using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
#nullable enable
namespace DuckDB.NET.Test;
public static class ModuleInit
{
	[ModuleInitializer]
	public static void Init()
	{
        if (GetRid() is not { } rid)
        {
            return;
        }

        if (NativeLibrary.TryLoad(Path.Join("runtimes", rid, "native", "duckdb"), out _))
        {
            return;
        }
		
        NativeLibrary.TryLoad(Path.Join("runtimes", rid, "native", "libduckdb"), out _);
    }

	private static string? GetRid()
	{
		if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
		{
			return Environment.Is64BitProcess 
				? "win-x64" 
				: "win-x86";
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
            return "osx-x64";
        }

        return null;
	}
}