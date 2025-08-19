using System;
using System.Numerics;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBHugeInt
{
    private static readonly BigInteger Base = BigInteger.Pow(2, 64);

    public static BigInteger HugeIntMinValue { get; } = BigInteger.Parse("-170141183460469231731687303715884105727");
    public static BigInteger HugeIntMaxValue { get; } = BigInteger.Parse("170141183460469231731687303715884105727");

    public DuckDBHugeInt(BigInteger value)
    {
        if (value < HugeIntMinValue || value > HugeIntMaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(value), $"value must be between {HugeIntMinValue} and {HugeIntMaxValue}");
        }

        var upper = (long)BigInteger.DivRem(value, Base, out var rem);

        if (rem < 0)
        {
            rem += Base;
            upper -= 1;
        }

        Upper = upper;
        Lower = (ulong)rem;
    }

    public DuckDBHugeInt(ulong lower, long upper)
    {
        Lower = lower;
        Upper = upper;
    }

    public ulong Lower { get; }
    public long Upper { get; }

    public BigInteger ToBigInteger()
    {
        return Upper * BigInteger.Pow(2, 64) + Lower;
    }
}

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBUHugeInt
{
    private static readonly BigInteger Base = BigInteger.Pow(2, 64);

    public static BigInteger HugeIntMinValue { get; } = BigInteger.Zero;
    public static BigInteger HugeIntMaxValue { get; } = BigInteger.Parse("340282366920938463463374607431768211455");

    public DuckDBUHugeInt(BigInteger value)
    {
        if (value < HugeIntMinValue || value > HugeIntMaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(value), $"value must be between {HugeIntMinValue} and {HugeIntMaxValue}");
        }

        var upper = (ulong)BigInteger.DivRem(value, Base, out var rem);

        Upper = upper;
        Lower = (ulong)rem;
    }

    public DuckDBUHugeInt(ulong lower, ulong upper)
    {
        Lower = lower;
        Upper = upper;
    }

    public ulong Lower { get; }
    public ulong Upper { get; }

    public BigInteger ToBigInteger()
    {
        return Upper * BigInteger.Pow(2, 64) + Lower;
    }
}