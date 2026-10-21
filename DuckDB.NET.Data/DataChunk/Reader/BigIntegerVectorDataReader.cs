using System.Buffers;

namespace DuckDB.NET.Data.DataChunk.Reader;

// HUGEINT, UHUGEINT and VARINT: the columns whose .NET type is BigInteger.
internal sealed class BigIntegerVectorDataReader : VectorDataReaderBase
{
    private const int VarIntHeaderSize = 3;

    internal unsafe BigIntegerVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
        StorageType = columnType switch
        {
            DuckDBType.HugeInt => typeof(DuckDBHugeInt),
            DuckDBType.UnsignedHugeInt => typeof(DuckDBUHugeInt),
            _ => null
        };
    }

    internal override byte ReadByte(ulong offset) => GetValidValue<byte>(offset);
    internal override short ReadInt16(ulong offset) => GetValidValue<short>(offset);
    internal override int ReadInt32(ulong offset) => GetValidValue<int>(offset);
    internal override long ReadInt64(ulong offset) => GetValidValue<long>(offset);

    protected override T GetValidValue<T>(ulong offset)
    {
        if (!TypeExtensions.IsIntegralNumericType<T>())
        {
            return base.GetValidValue<T>(offset);
        }

        return DuckDBType switch
        {
            DuckDBType.HugeInt => GetHugeInt<T>(offset),
            DuckDBType.UnsignedHugeInt => GetUnsignedHugeInt<T>(offset),
            _ => GetVarInt<T>(offset)
        };
    }

    // Every type is read with GetValidValue, so a list item, a map key or value and a struct field convert by the same
    // rules as a column. The class is sealed, so these calls are bound directly.
    internal override object GetValue(ulong offset, Type targetType)
    {
        // The .NET type of the three columns, which GetValue(ordinal) asks for.
        if (targetType == typeof(BigInteger)) return GetValidValue<BigInteger>(offset);

        // The provider-specific type of HUGEINT and UHUGEINT is the DuckDB struct, which is what the vector holds.
        if (targetType == typeof(DuckDBHugeInt) && DuckDBType == DuckDBType.HugeInt) return GetFieldData<DuckDBHugeInt>(offset);
        if (targetType == typeof(DuckDBUHugeInt) && DuckDBType == DuckDBType.UnsignedHugeInt) return GetFieldData<DuckDBUHugeInt>(offset);

        if (targetType == typeof(sbyte))  return GetValidValue<sbyte>(offset);
        if (targetType == typeof(byte))   return GetValidValue<byte>(offset);
        if (targetType == typeof(short))  return GetValidValue<short>(offset);
        if (targetType == typeof(ushort)) return GetValidValue<ushort>(offset);
        if (targetType == typeof(int))    return GetValidValue<int>(offset);
        if (targetType == typeof(uint))   return GetValidValue<uint>(offset);
        if (targetType == typeof(long))   return GetValidValue<long>(offset);
        if (targetType == typeof(ulong))  return GetValidValue<ulong>(offset);

        throw CannotCast(DuckDBType.ToString(), targetType);
    }

    // A HUGEINT is a 128-bit integer, which is what Int128 is, so an integer type is converted from that. The
    // range check is then a few comparisons. A BigInteger is made only when a BigInteger is asked for, and then
    // by ToBigInteger, like everywhere else.
    // The message of a refused read names the column type, which is what the caller knows from the schema.
    private T GetHugeInt<T>(ulong offset)
    {
        var value = GetFieldData<DuckDBHugeInt>(offset);

        if (typeof(T) == typeof(BigInteger))
        {
            return (T)(object)value.ToBigInteger();
        }

        try
        {
            return NumericConverter.Convert<Int128, T>(value.ToInt128());
        }
        catch (OverflowException)
        {
            throw CannotCast(DuckDBType.ToString(), typeof(T));
        }
    }

    private T GetUnsignedHugeInt<T>(ulong offset)
    {
        var value = GetFieldData<DuckDBUHugeInt>(offset);

        if (typeof(T) == typeof(BigInteger))
        {
            return (T)(object)value.ToBigInteger();
        }

        try
        {
            return NumericConverter.Convert<UInt128, T>(value.ToUInt128());
        }
        catch (OverflowException)
        {
            throw CannotCast(DuckDBType.ToString(), typeof(T));
        }
    }

    private unsafe T GetVarInt<T>(ulong offset)
    {
        var data = (DuckDBString*)DataPointer + offset;

        if (data->Length < VarIntHeaderSize + 1)
        {
            throw new DuckDBException("Invalid blob size for Varint.");
        }

        var buffer = new ReadOnlySpan<byte>(data->Data, data->Length);
        var isPositive = (buffer[0] & 0x80) != 0;
        var source = buffer.Slice(VarIntHeaderSize);

        byte[]? rented = null;

        try
        {
            if (isPositive) return NumericConverter.Convert<BigInteger, T>(new BigInteger(source, isUnsigned: true, isBigEndian: true));

            // Negative values need byte complementing — use stack for small payloads, pool for large.
            Span<byte> payload = source.Length <= 128
                ? stackalloc byte[source.Length]
                : (rented = ArrayPool<byte>.Shared.Rent(source.Length)).AsSpan(0, source.Length);

            for (var i = 0; i < source.Length; i++)
            {
                payload[i] = (byte)~source[i];
            }

            return NumericConverter.Convert<BigInteger, T>(-new BigInteger(payload, isUnsigned: true, isBigEndian: true));
        }
        catch (OverflowException)
        {
            throw CannotCast(DuckDBType.ToString(), typeof(T));
        }
        finally
        {
            if (rented != null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }
}
