using DuckDB.NET.Data.DataChunk.Writer;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data;

public class DuckDBAppenderRow : IDuckDBAppenderRow
{
    private readonly string qualifiedTableName;
    private readonly VectorDataWriterBase[] vectorWriters;
    private readonly DuckDBDataChunk dataChunk;
    private readonly Native.DuckDBAppender nativeAppender;

    internal ulong ChunkRowIndex { get; private set; }

    internal bool IsComplete => ValueCount == vectorWriters.Length;

    internal int ValueCount { get; private set; } = 0;

    internal DuckDBAppenderRow(string qualifiedTableName, VectorDataWriterBase[] vectorWriters,
                               ulong rowIndex, DuckDBDataChunk dataChunk, Native.DuckDBAppender nativeAppender)
    {
        this.qualifiedTableName = qualifiedTableName;
        this.vectorWriters = vectorWriters;
        ChunkRowIndex = rowIndex;
        this.dataChunk = dataChunk;
        this.nativeAppender = nativeAppender;
    }

    /// <summary>
    /// Re-targets this row instance at a new row index so the appender can reuse a single
    /// <see cref="DuckDBAppenderRow"/> instead of allocating one per row. The table name, vector
    /// writers, data chunk and native appender are stable for the lifetime of the appender, so only
    /// the row index and column cursor need to be reset.
    /// </summary>
    internal void Reset(ulong rowIndex)
    {
        ChunkRowIndex = rowIndex;
        ValueCount = 0;
    }

    internal void Invalidate()
    {
        ValueCount = vectorWriters.Length;
    }

    public void EndRow()
    {
        if (ValueCount < vectorWriters.Length)
        {
            throw new InvalidOperationException($"The table {qualifiedTableName} has {vectorWriters.Length} columns but you specified only {ValueCount} values");
        }
    }

    public IDuckDBAppenderRow AppendNullValue()
    {
        GetCurrentWriter().WriteNull(ChunkRowIndex);
        ValueCount++;
        return this;
    }

    public IDuckDBAppenderRow AppendValue(bool? value) => AppendValueInternalStruct(value);

    public IDuckDBAppenderRow AppendValue(byte[]? value) => AppendSpan(value);

    public IDuckDBAppenderRow AppendValue(Span<byte> value) => AppendSpan(value);

    public IDuckDBAppenderRow AppendValue(string? value) => AppendValueInternalClass(value);

    public IDuckDBAppenderRow AppendValue(decimal? value) => AppendValueInternalStruct(value);

    public IDuckDBAppenderRow AppendValue(Guid? value) => AppendValueInternalStruct(value);

    public IDuckDBAppenderRow AppendValue(BigInteger? value) => AppendValueInternalStruct(value);

    #region Append Signed Int

    public IDuckDBAppenderRow AppendValue(sbyte? value) => AppendValueInternalStruct(value);

    public IDuckDBAppenderRow AppendValue(short? value) => AppendValueInternalStruct(value);

    public IDuckDBAppenderRow AppendValue(int? value) => AppendValueInternalStruct(value);

    public IDuckDBAppenderRow AppendValue(long? value) => AppendValueInternalStruct(value);

    #endregion

    #region Append Unsigned Int

    public IDuckDBAppenderRow AppendValue(byte? value) => AppendValueInternalStruct(value);

    public IDuckDBAppenderRow AppendValue(ushort? value) => AppendValueInternalStruct(value);

    public IDuckDBAppenderRow AppendValue(uint? value) => AppendValueInternalStruct(value);

    public IDuckDBAppenderRow AppendValue(ulong? value) => AppendValueInternalStruct(value);

    #endregion

    #region Append Enum

    public IDuckDBAppenderRow AppendValue<TEnum>(TEnum? value) where TEnum : Enum
    {
        var writer = GetCurrentWriter();

        if (value != null)
        {
            writer.WriteValue(value, ChunkRowIndex);
        }
        else
        {
            writer.WriteNull(ChunkRowIndex);
        }

        ValueCount++;
        return this;
    }

    #endregion

    #region Append Float

    public IDuckDBAppenderRow AppendValue(float? value) => AppendValueInternalStruct(value);

    public IDuckDBAppenderRow AppendValue(double? value) => AppendValueInternalStruct(value);

    #endregion

    #region Append Temporal
    public IDuckDBAppenderRow AppendValue(DateOnly? value) => AppendValueInternalStruct(value);

    public IDuckDBAppenderRow AppendValue(TimeOnly? value) => AppendValueInternalStruct(value);

    public IDuckDBAppenderRow AppendValue(DuckDBDateOnly? value) => AppendValueInternalStruct(value);

    public IDuckDBAppenderRow AppendValue(DuckDBTimeOnly? value) => AppendValueInternalStruct(value);

    public IDuckDBAppenderRow AppendValue(DateTime? value) => AppendValueInternalStruct(value);

    public IDuckDBAppenderRow AppendValue(DateTimeOffset? value) => AppendValueInternalStruct(value);

    public IDuckDBAppenderRow AppendValue(TimeSpan? value) => AppendValueInternalStruct(value);

    #endregion

    #region Composite Types

    public IDuckDBAppenderRow AppendValue<T>(IEnumerable<T>? value) => AppendValueInternalClass(value);

    #endregion

    public IDuckDBAppenderRow AppendDefault()
    {
        GetCurrentWriter();

        var state = NativeMethods.Appender.DuckDBAppendDefaultToChunk(nativeAppender, dataChunk, ValueCount, ChunkRowIndex);

        if (state == DuckDBState.Error)
        {
            NativeMethods.Appender.DuckDBAppenderErrorData(nativeAppender).ThrowOnError();
        }

        ValueCount++;
        return this;
    }

    private DuckDBAppenderRow AppendValueInternalStruct<T>(T? value) where T : struct
    {
        var writer = GetCurrentWriter();

        if (value.HasValue)
        {
            writer.WriteValue(value.Value, ChunkRowIndex);
        }
        else
        {
            writer.WriteNull(ChunkRowIndex);
        }

        ValueCount++;
        return this;
    }

    private DuckDBAppenderRow AppendValueInternalClass<T>(T? value) where T : class
    {
        GetCurrentWriter().WriteValue(value, ChunkRowIndex);

        ValueCount++;
        return this;
    }

    private unsafe IDuckDBAppenderRow AppendSpan(Span<byte> val)
    {
        // A null byte[] or a default span has no memory at all and writes NULL. An empty array still has memory and is
        // an empty BLOB, so IsEmpty would be wrong here.
        if (Unsafe.IsNullRef(ref MemoryMarshal.GetReference(val)))
        {
            return AppendNullValue();
        }

        var writer = GetCurrentWriter();

        fixed (byte* pSource = val)
        {
            writer.AppendBlob(pSource, val.Length, ChunkRowIndex);
        }

        ValueCount++;
        return this;
    }

    // Returns the writer of the column the next value goes to, or throws when the row already has a value for
    // every column. The unsigned comparison on locals lets the JIT drop its own bounds check on the array, and
    // the throw lives in a separate method so that this one is small enough to be inlined.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private VectorDataWriterBase GetCurrentWriter()
    {
        var writers = vectorWriters;
        var index = ValueCount;

        if ((uint)index >= (uint)writers.Length)
        {
            ThrowColumnOutOfRange();
        }

        return writers[index];
    }

    [DoesNotReturn]
    private void ThrowColumnOutOfRange()
    {
        throw new IndexOutOfRangeException($"The table {qualifiedTableName} has {vectorWriters.Length} columns but you are trying to append value for column {ValueCount + 1}");
    }
}

public interface IDuckDBAppenderRow
{
    void EndRow();
    IDuckDBAppenderRow AppendNullValue();
    IDuckDBAppenderRow AppendValue(bool? value);
    IDuckDBAppenderRow AppendValue(byte[]? value);
    IDuckDBAppenderRow AppendValue(Span<byte> value);
    IDuckDBAppenderRow AppendValue(string? value);
    IDuckDBAppenderRow AppendValue(decimal? value);
    IDuckDBAppenderRow AppendValue(Guid? value);
    IDuckDBAppenderRow AppendValue(BigInteger? value);
    IDuckDBAppenderRow AppendValue(sbyte? value);
    IDuckDBAppenderRow AppendValue(short? value);
    IDuckDBAppenderRow AppendValue(int? value);
    IDuckDBAppenderRow AppendValue(long? value);
    IDuckDBAppenderRow AppendValue(byte? value);
    IDuckDBAppenderRow AppendValue(ushort? value);
    IDuckDBAppenderRow AppendValue(uint? value);
    IDuckDBAppenderRow AppendValue(ulong? value);
    IDuckDBAppenderRow AppendValue<TEnum>(TEnum? value) where TEnum : Enum;
    IDuckDBAppenderRow AppendValue(float? value);
    IDuckDBAppenderRow AppendValue(double? value);
    IDuckDBAppenderRow AppendValue(DuckDBDateOnly? value);
    IDuckDBAppenderRow AppendValue(DuckDBTimeOnly? value);
    IDuckDBAppenderRow AppendValue(DateTime? value);
    IDuckDBAppenderRow AppendValue(DateTimeOffset? value);
    IDuckDBAppenderRow AppendValue(TimeSpan? value);
    IDuckDBAppenderRow AppendValue<T>(IEnumerable<T>? value);
    IDuckDBAppenderRow AppendDefault();
}
