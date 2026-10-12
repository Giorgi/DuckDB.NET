using System.Numerics;

namespace DuckDB.NET.Native;

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBHugeInt
{
    // DuckDB's HUGEINT is a 128-bit two's complement integer, -2^127 to 2^127 - 1, which is what Int128 is.
    public static BigInteger HugeIntMinValue { get; } = Int128.MinValue;
    public static BigInteger HugeIntMaxValue { get; } = Int128.MaxValue;

    public DuckDBHugeInt(BigInteger value)
    {
        if (value < HugeIntMinValue || value > HugeIntMaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(value), $"value must be between {HugeIntMinValue} and {HugeIntMaxValue}");
        }

        // The two halves of the Int128 are the two halves of the HUGEINT, the sign included.
        var wide = (Int128)value;

        Upper = (long)(wide >> 64);
        Lower = (ulong)wide;
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
    public static BigInteger HugeIntMinValue { get; } = UInt128.MinValue;
    public static BigInteger HugeIntMaxValue { get; } = UInt128.MaxValue;

    public DuckDBUHugeInt(BigInteger value)
    {
        if (value < HugeIntMinValue || value > HugeIntMaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(value), $"value must be between {HugeIntMinValue} and {HugeIntMaxValue}");
        }

        var wide = (UInt128)value;

        Upper = (ulong)(wide >> 64);
        Lower = (ulong)wide;
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