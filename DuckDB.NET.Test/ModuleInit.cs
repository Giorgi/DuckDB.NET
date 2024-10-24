using DuckDB.NET.Test.Helpers;
using FluentAssertions;
using System;
using System.Runtime.CompilerServices;

#nullable enable
namespace DuckDB.NET.Test;
public static class ModuleInit
{
    [ModuleInitializer]
    public static void Init()
    {
        NativeLibraryHelper.TryLoad();

        AssertionOptions.AssertEquivalencyUsing(options => options.ComparingByMembers<DateTimeOffset>().Including(info => 
            info.Name == nameof(DateTimeOffset.Offset) || 
            info.Name == nameof(DateTimeOffset.TimeOfDay)));
    }
}
