using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Native;
using System;
using System.Collections.Generic;
using System.Numerics;

namespace DuckDB.NET.Data;

public class DuckDBAppenderRow : IDuckDBAppenderRow
{
    private int columnIndex = 0;
    private readonly string qualifiedTableName;
    private readonly VectorDataWriterBase[] vectorWriters;
    private readonly ulong rowIndex;
    private readonly DuckDBDataChunk dataChunk;
    private readonly Native.DuckDBAppender nativeAppender;

    internal DuckDBAppenderRow(string qualifiedTableName, VectorDataWriterBase[] vectorWriters,
                               ulong rowIndex, DuckDBDataChunk dataChunk, Native.DuckDBAppender nativeAppender)
    {
        this.qualifiedTableName = qualifiedTableName;
        this.vectorWriters = vectorWriters;
        this.rowIndex = rowIndex;
        this.dataChunk = dataChunk;
        this.nativeAppender = nativeAppender;
    }

    public void EndRow()
    {
        if (columnIndex < vectorWriters.Length)
        {
            throw new InvalidOperationException($"The table {qualifiedTableName} has {vectorWriters.Length} columns but you specified only {columnIndex} values");
        }
    }

    public IDuckDBAppenderRow AppendNullValue() => AppendValueInternal<int?>(null); //Doesn't matter what type T we pass to Append when passing null.

    public IDuckDBAppenderRow AppendValue(bool? value) => AppendValueInternal(value);

    public IDuckDBAppenderRow AppendValue(byte[]? value) => AppendSpan(value);

    public IDuckDBAppenderRow AppendValue(Span<byte> value) => AppendSpan(value);

    public IDuckDBAppenderRow AppendValue(string? value) => AppendValueInternal(value);

    public IDuckDBAppenderRow AppendValue(decimal? value) => AppendValueInternal(value);

    public IDuckDBAppenderRow AppendValue(Guid? value) => AppendValueInternal(value);

    public IDuckDBAppenderRow AppendValue(BigInteger? value) => AppendValueInternal(value);

    #region Append Signed Int

    public IDuckDBAppenderRow AppendValue(sbyte? value) => AppendValueInternal(value);

    public IDuckDBAppenderRow AppendValue(short? value) => AppendValueInternal(value);

    public IDuckDBAppenderRow AppendValue(int? value) => AppendValueInternal(value);

    public IDuckDBAppenderRow AppendValue(long? value) => AppendValueInternal(value);

    #endregion

    #region Append Unsigned Int

    public IDuckDBAppenderRow AppendValue(byte? value) => AppendValueInternal(value);

    public IDuckDBAppenderRow AppendValue(ushort? value) => AppendValueInternal(value);

    public IDuckDBAppenderRow AppendValue(uint? value) => AppendValueInternal(value);

    public IDuckDBAppenderRow AppendValue(ulong? value) => AppendValueInternal(value);

    #endregion

    #region Append Enum

    public IDuckDBAppenderRow AppendValue<TEnum>(TEnum? value) where TEnum : Enum => AppendValueInternal(value);

    #endregion

    #region Append Float

    public IDuckDBAppenderRow AppendValue(float? value) => AppendValueInternal(value);

    public IDuckDBAppenderRow AppendValue(double? value) => AppendValueInternal(value);

    #endregion

    #region Append Temporal
    public IDuckDBAppenderRow AppendValue(DateOnly? value) => AppendValueInternal(value);

    public IDuckDBAppenderRow AppendValue(TimeOnly? value) => AppendValueInternal(value);

    public IDuckDBAppenderRow AppendValue(DuckDBDateOnly? value) => AppendValueInternal(value);

    public IDuckDBAppenderRow AppendValue(DuckDBTimeOnly? value) => AppendValueInternal(value);


    public IDuckDBAppenderRow AppendValue(DateTime? value) => AppendValueInternal(value);

    public IDuckDBAppenderRow AppendValue(DateTimeOffset? value) => AppendValueInternal(value);

    public IDuckDBAppenderRow AppendValue(TimeSpan? value)
    {
        return AppendValueInternal(value);
    }

    #endregion

    #region Composite Types

    public IDuckDBAppenderRow AppendValue<T>(IEnumerable<T>? value) => AppendValueInternal(value);

    #endregion

    public IDuckDBAppenderRow AppendDefault()
    {
        CheckColumnAccess();

        var state = NativeMethods.Appender.DuckDBAppendDefaultToChunk(nativeAppender, dataChunk, columnIndex, rowIndex);

        if (state == DuckDBState.Error)
        {
            DuckDBAppender.ThrowLastError(nativeAppender);
        }

        columnIndex++;
        return this;
    }

    private DuckDBAppenderRow AppendValueInternal<T>(T? value)
    {
        CheckColumnAccess();

        vectorWriters[columnIndex].WriteValue(value, rowIndex);

        columnIndex++;

        return this;
    }

    private unsafe IDuckDBAppenderRow AppendSpan(Span<byte> val)
    {
        if (val == null)
        {
            return AppendNullValue();
        }

        CheckColumnAccess();

        fixed (byte* pSource = val)
        {
            vectorWriters[columnIndex].AppendBlob(pSource, val.Length, rowIndex);
        }

        columnIndex++;
        return this;
    }

    private void CheckColumnAccess()
    {
        if (columnIndex >= vectorWriters.Length)
        {
            throw new IndexOutOfRangeException($"The table {qualifiedTableName} has {vectorWriters.Length} columns but you are trying to append value for column {columnIndex + 1}");
        }
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
