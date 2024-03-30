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

namespace System.Diagnostics.CodeAnalysis
{
    [AttributeUsage(AttributeTargets.Method)]
    public class DoesNotReturnAttribute: Attribute { }

    [AttributeUsage(AttributeTargets.Method)]
    public class StackTraceHiddenAttribute : Attribute { }
}
#endif