using System.Linq;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.Extensions;

internal static class DecimalExtensions
{
    /// <summary>
    /// Pre-computed powers of 10 as BigInteger values for scales 0-38 (the full range of DuckDB DECIMAL).
    /// </summary>
    internal static readonly BigInteger[] BigIntPowersOfTen = Enumerable.Range(0, 39)
        .Select(i => BigInteger.Pow(10, i))
        .ToArray();

    /// <summary>
    /// The same powers of 10 as <see cref="Int128"/> values (10^38 fits). The reader divides by these,
    /// because a HUGEINT is an Int128 and dividing one allocates nothing, unlike a BigInteger.
    /// </summary>
    internal static readonly Int128[] Int128PowersOfTen = Enumerable.Range(0, 39)
        .Select(i => (Int128)BigInteger.Pow(10, i))
        .ToArray();

    /// <summary>
    /// The powers of 10 that 64 bits hold, 10^0 to 10^19. A decimal whose integer fits 64 bits is rescaled with these
    /// for a column of any storage size, which is the common case of a write.
    /// </summary>
    internal static readonly ulong[] UInt64PowersOfTen = Enumerable.Range(0, 20)
        .Select(i => (ulong)BigInteger.Pow(10, i))
        .ToArray();

    /// <summary>
    /// Maximum scale supported by .NET <see cref="decimal"/> (10^28 is the largest exact power of ten it can represent).
    /// Also, second perfect number in the sequence of perfect numbers, which is a fun coincidence.
    /// Why is it a fun coincidence? Because perfect numbers are those that are equal to the sum of their proper divisors, and 28 is the second such number (after 6).
    /// So, in a way, it's like the .NET decimal type is "perfectly" suited for handling scales up to 28, which adds a nice touch of mathematical elegance to the design.
    /// </summary>
    internal const int MaxDecimalScale = 28;

    /// <summary>
    /// Pre-computed powers of 10 as decimal values for scales 0-28 (the full range of <see cref="decimal"/>).
    /// Avoids <c>(decimal)Math.Pow(10, scale)</c> which loses precision for large scales
    /// due to the double intermediate representation.
    /// </summary>
    internal static readonly decimal[] PowersOfTen =
    [
        1m,
        10m,
        100m,
        1_000m,
        10_000m,
        100_000m,
        1_000_000m,
        10_000_000m,
        100_000_000m,
        1_000_000_000m,
        10_000_000_000m,
        100_000_000_000m,
        1_000_000_000_000m,
        10_000_000_000_000m,
        100_000_000_000_000m,
        1_000_000_000_000_000m,
        10_000_000_000_000_000m,
        100_000_000_000_000_000m,
        1_000_000_000_000_000_000m,
        10_000_000_000_000_000_000m,
        100_000_000_000_000_000_000m,
        1_000_000_000_000_000_000_000m,
        10_000_000_000_000_000_000_000m,
        100_000_000_000_000_000_000_000m,
        1_000_000_000_000_000_000_000_000m,
        10_000_000_000_000_000_000_000_000m,
        100_000_000_000_000_000_000_000_000m,
        1_000_000_000_000_000_000_000_000_000m,
        10_000_000_000_000_000_000_000_000_000m,
    ];

    /// <summary>
    /// The number of decimal digits of an integer that a decimal can hold (29 at most). Zero has one digit.
    /// </summary>
    internal static int CountDigits(UInt128 magnitude)
    {
        for (var digits = 1; digits <= MaxDecimalScale; digits++)
        {
            if (magnitude < (UInt128)Int128PowersOfTen[digits])
            {
                return digits;
            }
        }

        return MaxDecimalScale + 1;
    }

    extension(decimal value)
    {
        /// <summary>
        /// Takes the decimal apart. A decimal is an integer of 96 bits, a sign and a scale, and its value is the
        /// integer divided by 10^scale. Nothing is computed and nothing is allocated: the parts are read from the
        /// bits of the decimal. Everything that writes a decimal to DuckDB starts here.
        /// </summary>
        /// <param name="low">The lower 64 bits of the integer.</param>
        /// <param name="high">The upper 32 bits of the integer. Zero for every value of up to 19 digits.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void GetParts(out ulong low, out uint high, out bool isNegative, out byte scale)
        {
            Span<int> bits = stackalloc int[4];
            decimal.GetBits(value, bits);

            low = (uint)bits[0] | (ulong)(uint)bits[1] << 32;
            high = (uint)bits[2];
            isNegative = bits[3] < 0;
            scale = (byte)(bits[3] >> 16);
        }
    }
}
