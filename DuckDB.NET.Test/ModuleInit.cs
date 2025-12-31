using System.Runtime.CompilerServices;
using DuckDB.NET.Test.Helpers;

#nullable enable
namespace DuckDB.NET.Test;
public static class ModuleInit
{
    [ModuleInitializer]
    public static void Init()
    {
        NativeLibraryHelper.TryLoad();

        AssertionOptions.AssertEquivalencyUsing(options => options.Using<DateTimeOffset>(new DateTimeOffsetTimeComparer()));
    }

    class DateTimeOffsetTimeComparer : IEqualityComparer<DateTimeOffset>
    {
        public bool Equals(DateTimeOffset x, DateTimeOffset y)
        {
            return x.Offset == y.Offset && x.TimeOfDay == y.TimeOfDay;
        }

        public int GetHashCode(DateTimeOffset obj)
        {
            return obj.GetHashCode();
        }
    }
}
