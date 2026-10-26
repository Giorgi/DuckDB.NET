using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;

namespace DuckDB.NET.Data.DataChunk.Reader;

internal sealed class StringVectorDataReader : VectorDataReaderBase
{
    internal unsafe StringVectorDataReader(void* dataPointer, ulong* validityMaskPointer, DuckDBType columnType, string columnName) : base(dataPointer, validityMaskPointer, columnType, columnName)
    {
    }

    internal override string ReadString(ulong offset) => GetValidValue<string>(offset);
    internal override Stream ReadStream(ulong offset) => GetValidValue<Stream>(offset);

    protected override T GetValidValue<T>(ulong offset)
    {
        return DuckDBType switch
        {
            DuckDBType.Bit => GetBitString<T>(offset),
            DuckDBType.Blob => typeof(T) == typeof(byte[])
                ? (T)(object)GetBytes(offset)
                : (T)(object)GetStream(offset),
            DuckDBType.Varchar => (T)(object)GetString(offset),
            _ => base.GetValidValue<T>(offset)
        };
    }

    internal override object GetValueCore(ulong offset, Type targetType)
    {
        return DuckDBType switch
        {
            DuckDBType.Bit => GetBitString(offset),
            DuckDBType.Blob => targetType == typeof(byte[]) ? GetBytes(offset) : GetStream(offset),
            DuckDBType.Varchar => GetString(offset),
            _ => base.GetValueCore(offset, targetType)
        };
    }

    private T GetBitString<T>(ulong offset)
    {
        if (typeof(T) == typeof(string))
        {
            return (T)(object)GetBitString(offset);
        }

        if (typeof(T) == typeof(BitArray))
        {
            return (T)(object)GetBitStringAsBitArray(offset);
        }

        //Not GetValueStrict<T>: that comes back to this method
        return base.GetValidValue<T>(offset);
    }

    private string GetBitString(ulong offset)
    {
        var bitArray = GetBitStringAsBitArray(offset);

        return string.Create(bitArray.Length, bitArray, (chars, array) =>
        {
            for (int index = 0; index < array.Length; index++)
            {
                chars[index] = array[index] ? '1' : '0';
            }
        });
    }

    //Copied from https://github.com/duckdb/duckdb/blob/8a17511028d306561d88da9425f9e0e88dedd70c/src/common/types/bit.cpp#L63
    private unsafe BitArray GetBitStringAsBitArray(ulong offset)
    {
        var bits = (DuckDBString*)DataPointer + offset;
        var data = bits->Data;

        var bitLength = (bits->Length - 1) * 8 - *data;

        var output = new BitArray(bitLength);
        var outputIndex = 0;

        for (var bitIndex = *data; bitIndex < 8; bitIndex++)
        {
            output[outputIndex++] = (*(data + 1) & (1 << (7 - bitIndex))) > 0;
        }

        for (var byteIndex = 2; byteIndex < bits->Length; byteIndex++)
        {
            for (var bitIndex = 0; bitIndex < 8; bitIndex++)
            {
                output[outputIndex++] = (*(data + byteIndex) & (1 << (7 - bitIndex))) > 0;
            }
        }

        return output;
    }

    private unsafe string GetString(ulong offset)
    {
        var data = (DuckDBString*)DataPointer + offset;

        // The string constructor that takes an encoding validates more, wraps the pointer in a span and ends in the
        // same decoder, so this is the shorter way to it.
        return Encoding.UTF8.GetString((byte*)data->Data, data->Length);
    }

    // The value of the last GetChars call and its row in the chunk, so that reading a value in parts decodes it once.
    // Cleared when the reader moves to another chunk.
    private string? charsValue;
    private ulong charsRow;

    internal override void Reset(IntPtr vector)
    {
        charsValue = null;
        base.Reset(vector);
    }

    internal unsafe long GetChars(ulong offset, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        if (DuckDBType != DuckDBType.Varchar)
        {
            throw GetCharsNotSupported();
        }

        if (!IsValid(offset))
        {
            throw ValueIsNull();
        }

        if (buffer is null)
        {
            if (charsValue is not null && charsRow == offset)
            {
                return charsValue.Length;
            }

            // The length alone is counted from the UTF-8 bytes, without decoding them.
            var data = (DuckDBString*)DataPointer + offset;
            return CountUtf16Chars(new ReadOnlySpan<byte>((byte*)data->Data, data->Length));
        }

        if (charsValue is null || charsRow != offset)
        {
            charsValue = GetString(offset);
            charsRow = offset;
        }

        if (dataOffset >= charsValue.Length)
        {
            return 0;
        }

        var count = (int)Math.Min(length, charsValue.Length - dataOffset);
        charsValue.CopyTo((int)dataOffset, buffer, bufferOffset, count);
        return count;
    }

    // The UTF-16 length of valid UTF-8, which DuckDB guarantees for VARCHAR. ASCII bytes are one char each; otherwise
    // see CountNonAsciiUtf16Chars.
    internal static long CountUtf16Chars(ReadOnlySpan<byte> bytes)
    {
        // Ascii.IsValid stops at the first block with a non-ASCII byte.
        if (Ascii.IsValid(bytes))
        {
            return bytes.Length;
        }

        return CountNonAsciiUtf16Chars(bytes);
    }

    // One char for every byte that is not a continuation byte (0x80-0xBF), and a second one for every lead byte of a
    // four-byte sequence (0xF0-0xF4), which becomes a surrogate pair. As signed bytes, continuation bytes are -128..-65
    // and four-byte lead bytes -16..-12; -16..-1 is tested, because 0xF5-0xFF never occur in valid UTF-8.
    private static long CountNonAsciiUtf16Chars(ReadOnlySpan<byte> bytes)
    {
        long count = bytes.Length;
        var index = 0;

        if (Vector128.IsHardwareAccelerated)
        {
            ref var start = ref MemoryMarshal.GetReference(bytes);
            var continuationMax = Vector128.Create((sbyte)-65);
            var fourByteLeadMin = Vector128.Create((sbyte)-16);

            for (; index <= bytes.Length - Vector128<byte>.Count; index += Vector128<byte>.Count)
            {
                var block = Vector128.LoadUnsafe(ref start, (nuint)index).AsSByte();
                var continuation = Vector128.LessThanOrEqual(block, continuationMax);
                var fourByteLead = Vector128.GreaterThanOrEqual(block, fourByteLeadMin) & Vector128.LessThan(block, Vector128<sbyte>.Zero);

                count -= BitOperations.PopCount(continuation.ExtractMostSignificantBits());
                count += BitOperations.PopCount(fourByteLead.ExtractMostSignificantBits());
            }
        }

        for (; index < bytes.Length; index++)
        {
            var value = bytes[index];
            if ((value & 0xC0) == 0x80)
            {
                count--;
            }
            else if (value >= 0xF0)
            {
                count++;
            }
        }

        return count;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private NotSupportedException GetCharsNotSupported() => new($"GetChars is not supported for {DuckDBType} columns");

    [MethodImpl(MethodImplOptions.NoInlining)]
    private InvalidCastException ValueIsNull() => new($"Column '{ColumnName}' value is null");

    private unsafe Stream GetStream(ulong offset)
    {
        var data = (DuckDBString*)DataPointer + offset;

        return new UnmanagedMemoryStream((byte*)data->Data, data->Length, data->Length, FileAccess.Read);
    }

    private unsafe byte[] GetBytes(ulong offset)
    {
        var data = (DuckDBString*)DataPointer + offset;
        return new ReadOnlySpan<byte>((void*)data->Data, checked((int)data->Length)).ToArray();
    }
}
