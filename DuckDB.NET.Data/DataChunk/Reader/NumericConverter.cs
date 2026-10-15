using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Reader;

internal static class NumericConverter
{
    // Every test of TResult is a constant for the JIT, so one branch is left for each pair of types.
    // Throws OverflowException when the value does not fit.
    public static TResult Convert<TSource, TResult>(TSource value) where TSource : INumberBase<TSource>
    {
        if (typeof(TSource) == typeof(TResult))
            return Unsafe.As<TSource, TResult>(ref value);

        if (typeof(TResult) == typeof(byte))       return (TResult)(object)byte.CreateChecked(value);
        if (typeof(TResult) == typeof(sbyte))      return (TResult)(object)sbyte.CreateChecked(value);
        if (typeof(TResult) == typeof(short))      return (TResult)(object)short.CreateChecked(value);
        if (typeof(TResult) == typeof(ushort))     return (TResult)(object)ushort.CreateChecked(value);
        if (typeof(TResult) == typeof(int))        return (TResult)(object)int.CreateChecked(value);
        if (typeof(TResult) == typeof(uint))       return (TResult)(object)uint.CreateChecked(value);
        if (typeof(TResult) == typeof(long))       return (TResult)(object)long.CreateChecked(value);
        if (typeof(TResult) == typeof(ulong))      return (TResult)(object)ulong.CreateChecked(value);
        if (typeof(TResult) == typeof(float))      return (TResult)(object)float.CreateChecked(value);
        if (typeof(TResult) == typeof(double))     return (TResult)(object)double.CreateChecked(value);
        if (typeof(TResult) == typeof(decimal))    return (TResult)(object)decimal.CreateChecked(value);
        if (typeof(TResult) == typeof(BigInteger)) return (TResult)(object)BigInteger.CreateChecked(value);

        throw new InvalidCastException($"Cannot convert {typeof(TSource).Name} to {typeof(TResult).Name}");
    }
}
