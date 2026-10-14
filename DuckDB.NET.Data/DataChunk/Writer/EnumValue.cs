using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data.DataChunk.Writer;

// The number of an enum value, which is what an ENUM column stores: the position of a name in the list of names.
// It is widened from the integer under the enum as a cast widens it, so a negative value of a signed enum becomes a
// number above any position, and the writer refuses it.
//
// The type code is read once for each enum type. At Tier 1 a static readonly field of a class that is already
// initialized is a constant, so one line of the switch is left: a load of the value. The JIT judges inlining by the
// switch as written, before it folds it, and would call a method of one load for every value.
internal static class EnumValue<TEnum>
{
    private static readonly TypeCode TypeCode = Type.GetTypeCode(typeof(TEnum));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong ToUInt64(ref TEnum value) => TypeCode switch
    {
        TypeCode.SByte => (ulong)Unsafe.As<TEnum, sbyte>(ref value),
        TypeCode.Byte => Unsafe.As<TEnum, byte>(ref value),
        TypeCode.Int16 => (ulong)Unsafe.As<TEnum, short>(ref value),
        TypeCode.UInt16 => Unsafe.As<TEnum, ushort>(ref value),
        TypeCode.Int32 => (ulong)Unsafe.As<TEnum, int>(ref value),
        TypeCode.UInt32 => Unsafe.As<TEnum, uint>(ref value),
        TypeCode.Int64 or TypeCode.UInt64 => Unsafe.As<TEnum, ulong>(ref value),
        _ => EnumValue.CannotConvert(value)
    };
}

// The same for an enum that is already boxed, as a System.Enum or an object.
internal static class EnumValue
{
    public static ulong ToUInt64(Enum value) => value.GetTypeCode() switch
    {
        TypeCode.SByte => (ulong)Convert.ToSByte(value),
        TypeCode.Byte => Convert.ToByte(value),
        TypeCode.Int16 => (ulong)Convert.ToInt16(value),
        TypeCode.UInt16 => Convert.ToUInt16(value),
        TypeCode.Int32 => (ulong)Convert.ToInt32(value),
        TypeCode.UInt32 => Convert.ToUInt32(value),
        TypeCode.Int64 => (ulong)Convert.ToInt64(value),
        TypeCode.UInt64 => Convert.ToUInt64(value),
        _ => CannotConvert(value)
    };

    [DoesNotReturn]
    internal static ulong CannotConvert(object? value)
    {
        throw new InvalidOperationException($"Failed to convert the enum value {value} to ulong.");
    }
}
