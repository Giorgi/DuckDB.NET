using System.Globalization;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Writer;

// A decimal is an integer of 96 bits, a sign and a scale, and a DECIMAL column stores the integer that goes with its
// own scale. So a value is written by multiplying or dividing that integer by a power of ten. Digits after the
// point that the column has no room for are dropped. A value with more digits than the width of the column is
// refused: DuckDB does not check a chunk that is appended, so it would be stored and read back wrong.
// The reasons and the measurements are in DESIGN.md, decision 10.
internal sealed unsafe class DecimalVectorDataWriter(IntPtr vector, void* vectorData, DuckDBLogicalType logicalType, DuckDBType columnType)
    : VectorDataWriterBase(vector, vectorData, columnType)
{
    private readonly DuckDBType decimalType = NativeMethods.LogicalType.DuckDBDecimalInternalType(logicalType);
    private readonly byte scale = NativeMethods.LogicalType.DuckDBDecimalScale(logicalType);
    private readonly byte width = NativeMethods.LogicalType.DuckDBDecimalWidth(logicalType);

    // 10^width in two halves of 64 bits: the first integer with too many digits for the column.
    private readonly ulong limitLower = (ulong)DecimalExtensions.Int128PowersOfTen[NativeMethods.LogicalType.DuckDBDecimalWidth(logicalType)];
    private readonly ulong limitUpper = (ulong)(DecimalExtensions.Int128PowersOfTen[NativeMethods.LogicalType.DuckDBDecimalWidth(logicalType)] >> 64);

    // An integer that fits 64 bits, with a scale less than 20 away from the column's: one multiplication or division
    // of 64 bits. Every value of up to 19 digits has such an integer.
    internal override bool AppendDecimal(decimal value, ulong rowIndex)
    {
        value.GetParts(out var low, out var high, out var isNegative, out var valueScale);

        var powers = DecimalExtensions.UInt64PowersOfTen;
        var shift = scale - valueScale;

        if (high != 0 || Math.Abs(shift) >= powers.Length)
        {
            return AppendLongDecimal(value, rowIndex);
        }

        // The rescaled integer in two halves of 64 bits. Only a multiplication can fill the upper half.
        ulong lower, upper = 0;

        if (shift >= 0)
        {
            upper = Math.BigMul(low, powers[shift], out lower);
        }
        else
        {
            lower = low / powers[-shift];
        }

        if (upper > limitUpper || (upper == limitUpper && lower >= limitLower))
        {
            throw DoesNotFit(value);
        }

        return Store(lower, upper, isNegative, rowIndex);
    }

    // Stores an integer that fits the column, given as its magnitude in two halves of 64 bits and its sign.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool Store(ulong lower, ulong upper, bool isNegative, ulong rowIndex)
    {
        var signed = isNegative ? -(long)lower : (long)lower;

        return decimalType switch
        {
            DuckDBType.SmallInt => AppendValueInternal((short)signed, rowIndex),
            DuckDBType.Integer => AppendValueInternal((int)signed, rowIndex),
            DuckDBType.BigInt => AppendValueInternal(signed, rowIndex),
            // A negative number in two's complement: every bit flipped, plus one.
            DuckDBType.HugeInt when isNegative => AppendValueInternal(new DuckDBHugeInt(0 - lower, (long)(~upper + (lower == 0 ? 1UL : 0UL))), rowIndex),
            DuckDBType.HugeInt => AppendValueInternal(new DuckDBHugeInt(lower, (long)upper), rowIndex),
            _ => true
        };
    }

    // Every other decimal: the same steps on 128 bits, as the reader has them for a wide column. It is a method of
    // its own so that what it needs on the stack is not set up in AppendDecimal for every value.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private bool AppendLongDecimal(decimal value, ulong rowIndex)
    {
        // The digits in front of the point decide whether the value fits, and a decimal has 29 of them at most.
        // Tested first: a value that fits is below 10^width when it is rescaled, so the product below cannot
        // overflow the 128 bits.
        var integerDigits = width - scale;

        if (integerDigits <= DecimalExtensions.MaxDecimalScale && Math.Abs(value) >= DecimalExtensions.PowersOfTen[integerDigits])
        {
            throw DoesNotFit(value);
        }

        value.GetParts(out var low, out var high, out var isNegative, out var valueScale);

        var magnitude = new UInt128(high, low);
        var shift = scale - valueScale;
        var power = (UInt128)DecimalExtensions.Int128PowersOfTen[Math.Abs(shift)];

        var rescaled = shift >= 0 ? magnitude * power : magnitude / power;

        return Store((ulong)rescaled, (ulong)(rescaled >> 64), isNegative, rowIndex);
    }

    // A method, so that the message is not built in the stack frame of AppendDecimal.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private OverflowException DoesNotFit(decimal value)
    {
        return new OverflowException($"Value {value.ToString(CultureInfo.InvariantCulture)} has too many digits for a DECIMAL({width},{scale}) column");
    }
}
