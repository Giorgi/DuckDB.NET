#if !NET9_0_OR_GREATER
namespace System.Runtime.CompilerServices;

// .NET 9 has this attribute; the C# compiler recognizes it by its name, so the net8.0 build declares its own. A
// caller compiled with C# 13 or later honors it whatever the target framework of either side.
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Constructor | AttributeTargets.Property, AllowMultiple = false, Inherited = false)]
internal sealed class OverloadResolutionPriorityAttribute(int priority) : Attribute
{
    public int Priority { get; } = priority;
}
#endif
