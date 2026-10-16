using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk;

// The validity mask of a vector: one bit per row, 1 for valid, in 64-bit entries. The DuckDB C API documents
// this layout for direct access. A vector has no mask until one of its rows is NULL, so callers check the
// pointer first.
internal static unsafe class ValidityMask
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsValid(ulong* mask, ulong row) => (mask[row / 64] & (1UL << (int)(row % 64))) != 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void SetValid(ulong* mask, ulong row) => mask[row / 64] |= 1UL << (int)(row % 64);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void SetInvalid(ulong* mask, ulong row) => mask[row / 64] &= ~(1UL << (int)(row % 64));
}
