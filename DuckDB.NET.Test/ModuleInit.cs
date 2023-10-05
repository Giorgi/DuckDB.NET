using DuckDB.NET.Test.Helpers;
using System;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

#nullable enable
namespace DuckDB.NET.Test;
public static class ModuleInit
{
    [ModuleInitializer]
    public static void Init()
    {
        NativeLibraryHelper.TryLoad();
    }
}
