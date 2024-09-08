using DuckDB.NET.Data.Internal.Writer;
using DuckDB.NET.Native;
using System;
using System.Collections.Generic;
using System.Numerics;

namespace DuckDB.NET.Data;

public class DuckDBAppenderRow
{
    private int columnIndex = 0;
    private readonly string qualifiedTableName;
    private readonly VectorDataWriterBase[] vectorWriters;
    private readonly ulong rowIndex;

    internal DuckDBAppenderRow(string qualifiedTableName, VectorDataWriterBase[] vectorWriters, ulong rowIndex)
    {
        this.qualifiedTableName = qualifiedTableName;
        this.vectorWriters = vectorWriters;
        this.rowIndex = rowIndex;
    }

    public void EndRow()
    {
        if (columnIndex < vectorWriters.Length)
        {
            throw new InvalidOperationException($"The table {qualifiedTableName} has {vectorWriters.Length} columns but you specified only {columnIndex} values");
        }
    }

    public DuckDBAppenderRow AppendNullValue() => AppendValueInternal<int?>(null); //Doesn't matter what type T we pass to Append when passing null.

    public DuckDBAppenderRow AppendValue(bool? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(byte[]? value) => AppendSpan(value);

    public DuckDBAppenderRow AppendValue(Span<byte> value) => AppendSpan(value);

    public DuckDBAppenderRow AppendValue(string? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(decimal? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(Guid? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(BigInteger? value) => AppendValueInternal(value);

    #region Append Signed Int

    public DuckDBAppenderRow AppendValue(sbyte? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(short? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(int? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(long? value) => AppendValueInternal(value);

    #endregion

    #region Append Unsigned Int

    public DuckDBAppenderRow AppendValue(byte? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(ushort? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(uint? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(ulong? value) => AppendValueInternal(value);

    #endregion

    #region Append Float

    public DuckDBAppenderRow AppendValue(float? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(double? value) => AppendValueInternal(value);

    #endregion

    #region Append Temporal
#if NET6_0_OR_GREATER
    public DuckDBAppenderRow AppendValue(DateOnly? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(TimeOnly? value) => AppendValueInternal(value);
#endif

    public DuckDBAppenderRow AppendValue(DuckDBDateOnly? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(DuckDBTimeOnly? value) => AppendValueInternal(value);


    public DuckDBAppenderRow AppendValue(DateTime? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(DateTimeOffset? value) => AppendValueInternal(value);

    public DuckDBAppenderRow AppendValue(TimeSpan? value)
    {
        return AppendValueInternal(value);
    }

    #endregion

    #region Composite Types

    public DuckDBAppenderRow AppendValue<T>(IEnumerable<T>? value) => AppendValueInternal(value);

    #endregion

    private DuckDBAppenderRow AppendValueInternal<T>(T? value)
    {
        CheckColumnAccess();

        vectorWriters[columnIndex].WriteValue(value, rowIndex);

        columnIndex++;

        return this;
    }

    private unsafe DuckDBAppenderRow AppendSpan(Span<byte> val)
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
