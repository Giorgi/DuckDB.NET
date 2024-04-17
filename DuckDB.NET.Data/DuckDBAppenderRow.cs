using DuckDB.NET.Data.Internal.Writer;
using DuckDB.NET.Native;
using System;
using System.Numerics;

namespace DuckDB.NET.Data;

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

#if NET6_0_OR_GREATER
    public DuckDBAppenderRow AppendValue(byte[]? value) => AppendSpan(value);

    public DuckDBAppenderRow AppendValue(Span<byte> value) => AppendSpan(value);
#endif

    public DuckDBAppenderRow AppendValue(string? value)
    {
        if (value == null)
        {
            return AppendNullValue();
        }
     
        CheckColumnAccess();

        using var unmanagedString = value.ToUnmanagedString();

        vectors[columnIndex].AppendString(unmanagedString, rowIndex);

        columnIndex++;

        return this;
    }

    public DuckDBAppenderRow AppendNullValue()
    {
        Append<int>(null);
        return this;
    }

    public DuckDBAppenderRow AppendValue(BigInteger? value, bool unsigned = false)
    {
        if (value == null)
        {
            return AppendNullValue();
        }

        if (unsigned)
        {
            Append<DuckDBUHugeInt>(new DuckDBUHugeInt(value.Value));
        }
        else
        {
            Append<DuckDBHugeInt>(new DuckDBHugeInt(value.Value));
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
    public DuckDBAppenderRow AppendValue(DateOnly? value) => Append(value == null ? (DuckDBDate?)null : NativeMethods.DateTimeHelpers.DuckDBToDate(value.Value));

    public DuckDBAppenderRow AppendValue(TimeOnly? value) => Append(value == null ? (DuckDBTime?)null : NativeMethods.DateTimeHelpers.DuckDBToTime(value.Value));
#else
    public DuckDBAppenderRow AppendValue(DuckDBDateOnly? value) => Append(value == null ? (DuckDBDate?)null : NativeMethods.DateTimeHelpers.DuckDBToDate(value.Value));

    public DuckDBAppenderRow AppendValue(DuckDBTimeOnly? value) => Append(value == null ? (DuckDBTime?)null : NativeMethods.DateTimeHelpers.DuckDBToTime(value.Value));
#endif

    public DuckDBAppenderRow AppendValue(DateTime? value) => Append(value == null ? (DuckDBTimestampStruct?)null : NativeMethods.DateTimeHelpers.DuckDBToTimestamp(DuckDBTimestamp.FromDateTime(value.Value)));

    #endregion

    private DuckDBAppenderRow Append<T>(T? value) where T : unmanaged
    {
        CheckColumnAccess();

        if (value == null)
        {
            vectors[columnIndex].AppendNull(rowIndex);
        }
        else
        {
            vectors[columnIndex].AppendValue(value.Value, rowIndex);
        }

        columnIndex++;

        return this;
    }

#if NET6_0_OR_GREATER
    private unsafe DuckDBAppenderRow AppendSpan(Span<byte> val)
    {
        if (val == null)
        {
            return AppendNullValue();
        }

        CheckColumnAccess();
        
        fixed (byte* pSource = val)
        {
            vectors[columnIndex].AppendBlob(pSource, val.Length, rowIndex);
        }

        columnIndex++;
        return this;
    }

#endif

    private void CheckColumnAccess()
    {
        if (columnIndex >= vectors.Length)
        {
            throw new IndexOutOfRangeException($"The table {qualifiedTableName} has {vectors.Length} columns but you are trying to append value for column {columnIndex + 1}");
        }
    }
}