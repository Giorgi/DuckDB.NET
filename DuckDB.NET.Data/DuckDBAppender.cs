using DuckDB.NET.Native;
using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;

namespace DuckDB.NET.Data;

public class DuckDBAppender : IDisposable
{
    private static readonly ulong DuckDBVectorSize = NativeMethods.Helpers.DuckDBVectorSize();

    private bool closed;
    private readonly Native.DuckDBAppender nativeAppender;
    private readonly string qualifiedTableName;

    private ulong rowCount;

    private readonly DuckDBLogicalType[] logicalTypes;
    private readonly DuckDBDataChunk dataChunk;
    private readonly DataChunkVectorWriter[] vectorWriters;

    internal unsafe DuckDBAppender(Native.DuckDBAppender appender, string qualifiedTableName)
    {
        nativeAppender = appender;
        this.qualifiedTableName = qualifiedTableName;

        var columnCount = NativeMethods.Appender.DuckDBAppenderColumnCount(nativeAppender);

        vectorWriters = new DataChunkVectorWriter[columnCount];
        logicalTypes = new DuckDBLogicalType[columnCount];
        var logicalTypeHandles = new IntPtr[columnCount];

        for (ulong index = 0; index < columnCount; index++)
        {
            logicalTypes[index] = NativeMethods.Appender.DuckDBAppenderColumnType(nativeAppender, index);
            logicalTypeHandles[index] = logicalTypes[index].DangerousGetHandle();
        }

        dataChunk = NativeMethods.DataChunks.DuckDBCreateDataChunk(logicalTypeHandles, columnCount);
    }

    public DuckDBAppenderRow CreateRow()
    {
        if (closed)
        {
            throw new InvalidOperationException("Appender is already closed");
        }

        if (rowCount % DuckDBVectorSize == 0)
        {
            AppendDataChunk();

            InitVectorWriters();
            rowCount = 0;
        }

        rowCount++;
        return new DuckDBAppenderRow(nativeAppender, qualifiedTableName, vectorWriters, rowCount - 1);
    }

    public void Close()
    {
        closed = true;

        try
        {
            AppendDataChunk();

            foreach (var logicalType in logicalTypes)
            {
                logicalType.Dispose();
            }

            var state = NativeMethods.Appender.DuckDBAppenderClose(nativeAppender);
            if (!state.IsSuccess())
            {
                ThrowLastError(nativeAppender);
            }

            dataChunk.Dispose();
        }
        finally
        {
            nativeAppender.Close();
        }
    }

    public void Dispose()
    {
        if (!closed)
        {
            Close();
        }
    }

    private unsafe void InitVectorWriters()
    {
        for (long index = 0; index < vectorWriters.LongLength; index++)
        {
            var vector = NativeMethods.DataChunks.DuckDBDataChunkGetVector(dataChunk, index);
            var vectorData = NativeMethods.Vectors.DuckDBVectorGetData(vector);

            vectorWriters[index] = new DataChunkVectorWriter(vector, vectorData);
        }
    }

    private void AppendDataChunk()
    {
        NativeMethods.DataChunks.DuckDBDataChunkSetSize(dataChunk, rowCount);
        var state = NativeMethods.Appender.DuckDBAppendDataChunk(nativeAppender, dataChunk);

        if (!state.IsSuccess())
        {
            ThrowLastError(nativeAppender);
        }

        NativeMethods.DataChunks.DuckDBDataChunkReset(dataChunk);
    }

    [DoesNotReturn]
    [StackTraceHidden]
    internal static void ThrowLastError(Native.DuckDBAppender appender)
    {
        var errorMessage = NativeMethods.Appender.DuckDBAppenderError(appender).ToManagedString(false);

        throw new DuckDBException(errorMessage);
    }
}

public class DuckDBAppenderRow
{
    private int columnIndex = 0;
    private readonly Native.DuckDBAppender appender;
    private readonly string qualifiedTableName;
    private readonly DataChunkVectorWriter[] vectors;
    private readonly ulong rowIndex;

    internal DuckDBAppenderRow(Native.DuckDBAppender appender, string qualifiedTableName, DataChunkVectorWriter[] vectors, ulong rowIndex)
    {
        this.appender = appender;
        this.qualifiedTableName = qualifiedTableName;
        this.vectors = vectors;
        this.rowIndex = rowIndex;
    }

    public void EndRow()
    {
        if (columnIndex < vectors.Length)
        {
            throw new InvalidOperationException($"The table {qualifiedTableName} has {vectors.Length} columns but you specified only {columnIndex} values");
        }
    }

    public DuckDBAppenderRow AppendValue(bool? value) => Append(value);

    public DuckDBAppenderRow AppendValue(byte[]? value) => Append(value);

#if NET6_0_OR_GREATER
    public DuckDBAppenderRow AppendValue(Span<byte> value) => AppendSpan(value);
#endif

    public DuckDBAppenderRow AppendValue(string? value)
    {
        if (value == null)
        {
            return AppendNullValue();
        }

        using var unmanagedString = value.ToUnmanagedString();
        return Append(unmanagedString);
    }

    public DuckDBAppenderRow AppendNullValue() => Append<object>(null);

    public DuckDBAppenderRow AppendValue(BigInteger? value, bool unsigned = false)
    {
        if (value == null)
        {
            return AppendNullValue();
        }

        if (unsigned)
        {
            Append(new DuckDBUHugeInt(value.Value));
        }
        else
        {
            Append(new DuckDBHugeInt(value.Value));
        }

        return this;
    }

    #region Append Signed Int

    public DuckDBAppenderRow AppendValue(sbyte? value) => Append(value);

    public DuckDBAppenderRow AppendValue(short? value) => Append(value);

    public DuckDBAppenderRow AppendValue(int? value) => Append(value);

    public DuckDBAppenderRow AppendValue(long? value) => Append(value);

    #endregion

    #region Append Unsigned Int

    public DuckDBAppenderRow AppendValue(byte? value) => Append(value);

    public DuckDBAppenderRow AppendValue(ushort? value) => Append(value);

    public DuckDBAppenderRow AppendValue(uint? value) => Append(value);

    public DuckDBAppenderRow AppendValue(ulong? value) => Append(value);

    #endregion

    #region Append Float

    public DuckDBAppenderRow AppendValue(float? value) => Append(value);

    public DuckDBAppenderRow AppendValue(double? value) => Append(value);

    #endregion

    #region Append Temporal
#if NET6_0_OR_GREATER
    public DuckDBAppenderRow AppendValue(DateOnly? value) => Append(value);

    public DuckDBAppenderRow AppendValue(TimeOnly? value) => Append(value);
#else
    public DuckDBAppenderRow AppendValue(DuckDBDateOnly? value) => Append(value);

    public DuckDBAppenderRow AppendValue(DuckDBTimeOnly? value) => Append(value);
#endif

    public DuckDBAppenderRow AppendValue(DateTime? value) => Append(value);

    #endregion

    private DuckDBAppenderRow Append<T>(T? value)
    {
        if (columnIndex >= vectors.Length)
        {
            throw new IndexOutOfRangeException($"The table {qualifiedTableName} has {vectors.Length} columns but you are trying to append value for column {columnIndex + 1}");
        }

        var state = value switch
        {
            null => vectors[columnIndex].AppendNull(rowIndex),
            bool val => vectors[columnIndex].AppendValue<byte>((byte)(val ? 1 : 0), rowIndex),
            SafeUnmanagedMemoryHandle val => vectors[columnIndex].AppendString(val, rowIndex),

            sbyte val => vectors[columnIndex].AppendValue(val, rowIndex),
            short val => vectors[columnIndex].AppendValue(val, rowIndex),
            int val => vectors[columnIndex].AppendValue(val, rowIndex),
            long val => vectors[columnIndex].AppendValue(val, rowIndex),

            byte val => vectors[columnIndex].AppendValue(val, rowIndex),
            ushort val => vectors[columnIndex].AppendValue(val, rowIndex),
            uint val => vectors[columnIndex].AppendValue(val, rowIndex),
            ulong val => vectors[columnIndex].AppendValue(val, rowIndex),

            float val => vectors[columnIndex].AppendValue(val, rowIndex),
            double val => vectors[columnIndex].AppendValue(val, rowIndex),

            DuckDBHugeInt val => vectors[columnIndex].AppendValue(val, rowIndex),
            DuckDBUHugeInt val => vectors[columnIndex].AppendValue(val, rowIndex),

            DateTime val => vectors[columnIndex].AppendValue(NativeMethods.DateTimeHelpers.DuckDBToTimestamp(DuckDBTimestamp.FromDateTime(val)), rowIndex),
#if NET6_0_OR_GREATER
            DateOnly val => vectors[columnIndex].AppendValue(NativeMethods.DateTimeHelpers.DuckDBToDate(val), rowIndex),
            TimeOnly val => vectors[columnIndex].AppendValue(NativeMethods.DateTimeHelpers.DuckDBToTime(val), rowIndex),
#else
            DuckDBDateOnly val => vectors[columnIndex].AppendValue(NativeMethods.DateTimeHelpers.DuckDBToDate(val), rowIndex),
            DuckDBTimeOnly val => vectors[columnIndex].AppendValue(NativeMethods.DateTimeHelpers.DuckDBToTime(val), rowIndex),
#endif
            byte[] val => AppendByteArray(val),
            _ => throw new InvalidOperationException($"Unsupported type {typeof(T).Name}")
        };

        if (!state.IsSuccess())
        {
            DuckDBAppender.ThrowLastError(appender);
        }

        columnIndex++;

        return this;
    }

    private unsafe DuckDBState AppendByteArray(byte[] val)
    {
        fixed (byte* pSource = val)
        {
            return vectors[columnIndex].AppendBlob(pSource, val.Length, rowIndex);
        }
    }

#if NET6_0_OR_GREATER
    private unsafe DuckDBAppenderRow AppendSpan(Span<byte> val)
    {
        fixed (byte* pSource = val)
        {
            vectors[columnIndex].AppendBlob(pSource, val.Length, rowIndex);
        }

        columnIndex++;
        return this;
    }
#endif
}
