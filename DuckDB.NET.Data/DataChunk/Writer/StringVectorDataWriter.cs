using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;

namespace DuckDB.NET.Data.DataChunk.Writer;

// A string is encoded to UTF-8 here and not by the marshaller of the native call. DuckDB keeps a string of up to 12
// bytes inside its 16-byte value, so a short string is written straight into the vector, with no native call. It
// is the reader's DuckDBString in the other direction. A longer one is handed to DuckDB with its length, and
// without DuckDB's check of the UTF-8: what the encoder writes is always valid.
internal sealed unsafe class StringVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    private const int InlineLength = 12;

    // The longest string, in characters, that is encoded on the stack: three bytes for a character at most.
    private const int StackCharacters = 170;
    private const int StackBytes = StackCharacters * 3;

    internal override bool AppendString(string value, ulong rowIndex)
    {
        // A string with a NUL character is not inlined either: it goes the long way, which ends it there.
        if (value.Length > InlineLength || !DuckDBString.TryCreateInlined(value, out var inline))
        {
            return AppendLongString(value, rowIndex);
        }

        return AppendValueInternal(inline, rowIndex);
    }

    // A method of its own, so that the buffer is not set up on the stack for a short string.
    [MethodImpl(MethodImplOptions.NoInlining)]
    [SkipLocalsInit]
    private bool AppendLongString(string value, ulong rowIndex)
    {
        byte[]? rented = null;

        var buffer = value.Length <= StackCharacters
            ? stackalloc byte[StackBytes]
            : rented = ArrayPool<byte>.Shared.Rent(Encoding.UTF8.GetByteCount(value));

        var length = Encoding.UTF8.GetBytes(value, buffer);

        // The string ends at its first NUL character, as it did when DuckDB measured it as a C string.
        var terminator = buffer[..length].IndexOf((byte)0);

        if (terminator >= 0)
        {
            length = terminator;
        }

        fixed (byte* pointer = buffer)
        {
            NativeMethods.Vectors.DuckDBUnsafeVectorAssignStringElementLength(Vector, rowIndex, pointer, length);
        }

        if (rented != null)
        {
            ArrayPool<byte>.Shared.Return(rented);
        }

        return true;
    }

    internal override bool AppendBlob(byte* value, int length, ulong rowIndex)
    {
        NativeMethods.Vectors.DuckDBVectorAssignStringElementLength(Vector, rowIndex, value, length);
        return true;
    }
}
