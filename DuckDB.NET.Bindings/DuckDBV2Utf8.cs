using System.Buffers;

namespace DuckDB.NET.Native;

/// <summary>
/// UTF-8 strings for the C API V2, which takes and returns them as a pointer and a length, with no null terminator.
/// </summary>
public static class DuckDBV2Utf8
{
    /// <summary>
    /// Reads a string that V2 returned as a borrowed pointer and a length. The bytes stay owned by DuckDB.
    /// </summary>
    public static unsafe string ToManaged(byte* text, ulong length)
    {
        if (text == null || length == 0)
        {
            return string.Empty;
        }

        return Encoding.UTF8.GetString(text, checked((int)length));
    }

    /// <summary>
    /// Encodes <paramref name="text"/> as UTF-8 into a buffer rented from <see cref="ArrayPool{T}.Shared"/>, to pass
    /// to V2 as a pointer and a length. Return the buffer with <see cref="Utf8Buffer.Dispose"/>.
    /// </summary>
    public static Utf8Buffer FromManaged(string text) => new(text);

    /// <summary>
    /// A UTF-8 encoding of a string in a rented buffer. The first <see cref="Length"/> bytes of <see cref="Bytes"/>
    /// hold it, with no null terminator.
    /// </summary>
    public readonly struct Utf8Buffer : IDisposable
    {
        private readonly byte[] buffer;

        internal Utf8Buffer(string text)
        {
            buffer = ArrayPool<byte>.Shared.Rent(Math.Max(1, Encoding.UTF8.GetMaxByteCount(text.Length)));
            Length = Encoding.UTF8.GetBytes(text, buffer);
        }

        /// <summary>The bytes, to pin with <c>fixed</c>. Longer than <see cref="Length"/>.</summary>
        public byte[] Bytes => buffer;

        public int Length { get; }

        public ReadOnlySpan<byte> Span => buffer.AsSpan(0, Length);

        public void Dispose()
        {
            if (buffer != null)
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }
}
