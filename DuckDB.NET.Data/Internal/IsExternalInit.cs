using System.Collections;
using System.Collections.Generic;

namespace System.Runtime.CompilerServices
{
    internal static class IsExternalInit { }
}

#if !NET6_0_OR_GREATER
public static class KeyValuePairExtensions
{
    public static void Deconstruct<TKey, TValue>(this KeyValuePair<TKey, TValue> keyValuePair, out TKey key, out TValue value)
    {
        key = keyValuePair.Key;
        value = keyValuePair.Value;
    }
}

static class IEnumerableExtensions
{
    public static bool TryGetNonEnumeratedCount<T>(this IEnumerable target, out int count)
    {
        if (target is ICollection collection)
        {
            count = collection.Count;
            return true;
        }

        count = 0;
        return false;
    }
}

namespace System.Diagnostics.CodeAnalysis
{
    [AttributeUsage(AttributeTargets.Method)]
    class DoesNotReturnAttribute: Attribute { }

    [AttributeUsage(AttributeTargets.Method)]
    class StackTraceHiddenAttribute : Attribute { }
}
#endif