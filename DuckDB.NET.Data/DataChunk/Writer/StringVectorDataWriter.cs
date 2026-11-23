using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;
using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.DataChunk.Writer;

// A string is encoded to UTF-8 here and not by the marshaller of the native call. DuckDB keeps a string of up to 12
// bytes inside its 16-byte value, so a short string is written straight into the vector, with no native call. It
// is the reader's DuckDBString in the other direction. A longer one goes into the vector's arena, and the value is
// written here: the length, the first 4 bytes and the pointer. V2 has no call that assigns a string; V1's checked the
// UTF-8, and what the encoder writes is always valid.
internal sealed unsafe class StringVectorDataWriter(IntPtr vector, void* vectorData, DuckDBType columnType) : VectorDataWriterBase(vector, vectorData, columnType)
{
    private const int InlineLength = 12;

    // Borrowed from the vector on the first long value, and fetched again after the vector is reallocated.
    private _duckdb_v2_arena* arena;

    // The longest string, in characters, that is encoded on the stack: three bytes for a character at most.
    private const int StackCharacters = 170;
    private const int StackBytes = StackCharacters * 3;

    internal override bool AppendString(string value, ulong rowIndex)
    {
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

        fixed (byte* pointer = buffer)
        {
            WriteOutOfLine(pointer, length, rowIndex);
        }

        if (rented != null)
        {
            ArrayPool<byte>.Shared.Return(rented);
        }

        return true;
    }

    internal override bool AppendBlob(byte* value, int length, ulong rowIndex)
    {
        if (length > InlineLength)
        {
            WriteOutOfLine(value, length, rowIndex);
            return true;
        }

        // Inlined: the length, then the bytes, then zeros. DuckDB compares and hashes a short value by all 16 bytes.
        var slot = (byte*)VectorData + rowIndex * 16;
        *(uint*)slot = (uint)length;
        new Span<byte>(slot + 4, InlineLength).Clear();
        Buffer.MemoryCopy(value, slot + 4, InlineLength, length);
        return true;
    }

    // More than 12 bytes: copied into the arena, and the value holds the length, the first 4 bytes and the pointer.
    private void WriteOutOfLine(byte* value, int length, ulong rowIndex)
    {
        if (arena == null)
        {
            arena = DuckDBV2Vectors.GetArena((_duckdb_v2_vector*)Vector);
        }

        _duckdb_v2_error_info* error = null;
        byte* target;
        NativeMethodsV2.duckdb_v2_arena_allocate(arena, (ulong)length, &target, &error).ThrowIfFailed(ref error);

        Buffer.MemoryCopy(value, target, length, length);

        var slot = (byte*)VectorData + rowIndex * 16;
        *(uint*)slot = (uint)length;
        *(uint*)(slot + 4) = *(uint*)value;
        *(byte**)(slot + 8) = target;
    }

    internal override void InitializeWriter()
    {
        arena = null;
        base.InitializeWriter();
    }
}
