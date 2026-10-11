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
            DuckDBType.HugeInt => GetBigInteger<T>(offset, false),
            DuckDBType.UnsignedHugeInt => GetBigInteger<T>(offset, true),
            _ => GetVarInt<T>(offset)
        };
    }

    internal override object GetValue(ulong offset, Type targetType)
    {
        object value = DuckDBType switch
        {
            // The provider-specific type of these two is the DuckDB struct, which is what the vector holds.
            DuckDBType.HugeInt => targetType == typeof(DuckDBHugeInt) ? GetFieldData<DuckDBHugeInt>(offset) : GetBigInteger(offset, false),
            DuckDBType.UnsignedHugeInt => targetType == typeof(DuckDBUHugeInt) ? GetFieldData<DuckDBUHugeInt>(offset) : GetBigInteger(offset, true),
            _ => GetVarInt<BigInteger>(offset)
        };

        if (value.GetType() == targetType)
        {
            return value;
        }

        if (targetType.IsNumeric())
        {
            try
            {
                return Convert.ChangeType(value, targetType);
            }
            catch (OverflowException)
            {
                throw CannotCast(value.GetType(), targetType);
            }
        }

        throw CannotCast(value.GetType(), targetType);
    }

    private BigInteger GetBigInteger(ulong offset, bool unsigned)
    {
        return unsigned ? GetFieldData<DuckDBUHugeInt>(offset).ToBigInteger() : GetFieldData<DuckDBHugeInt>(offset).ToBigInteger();
    }

    private T GetBigInteger<T>(ulong offset, bool unsigned)
    {
        var bigInteger = GetBigInteger(offset, unsigned);

        try
        {
            return NumericConverter.Convert<BigInteger, T>(bigInteger);
        }
        catch (OverflowException)
        {
            throw CannotCast(typeof(BigInteger), typeof(T));
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
            throw CannotCast(typeof(BigInteger), typeof(T));
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
