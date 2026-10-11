using System.Numerics;

namespace DuckDB.NET.Native;

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBHugeInt
{
    private static readonly BigInteger Base = BigInteger.Pow(2, 64);

    // DuckDB's HUGEINT is a 128-bit two's complement integer: -2^127 to 2^127 - 1.
    // Parsed with the invariant culture: the current one may write the negative sign in another way.
    public static BigInteger HugeIntMinValue { get; } = BigInteger.Parse("-170141183460469231731687303715884105728", CultureInfo.InvariantCulture);
    public static BigInteger HugeIntMaxValue { get; } = BigInteger.Parse("170141183460469231731687303715884105727", CultureInfo.InvariantCulture);

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

    // A HUGEINT is a 128-bit integer, which is what Int128 is, and BigInteger converts from it directly.
    // Upper * Base + Lower gives the same value through two conversions, a multiplication and an addition.
    public BigInteger ToBigInteger()
    {
        return new Int128((ulong)Upper, Lower);
    }
}

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBUHugeInt
{
    private static readonly BigInteger Base = BigInteger.Pow(2, 64);

    public static BigInteger HugeIntMinValue { get; } = BigInteger.Zero;
    public static BigInteger HugeIntMaxValue { get; } = BigInteger.Parse("340282366920938463463374607431768211455", CultureInfo.InvariantCulture);

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
        return new UInt128(Upper, Lower);
    }
}