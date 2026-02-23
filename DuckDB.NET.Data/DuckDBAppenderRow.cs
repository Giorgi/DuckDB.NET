using DuckDB.NET.Data.DataChunk.Writer;

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

    public IDuckDBAppenderRow AppendNullValue()
    {
        CheckColumnAccess();
        vectorWriters[columnIndex].WriteNull(rowIndex);
        columnIndex++;
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
        CheckColumnAccess();

        if (value != null)
        {
            vectorWriters[columnIndex].WriteValue(value, rowIndex);
        }
        else
        {
            vectorWriters[columnIndex].WriteNull(rowIndex);
        }

        columnIndex++;
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
        CheckColumnAccess();

        var state = NativeMethods.Appender.DuckDBAppendDefaultToChunk(nativeAppender, dataChunk, columnIndex, rowIndex);

        if (state == DuckDBState.Error)
        {
            DuckDBAppender.ThrowLastError(nativeAppender);
        }

        columnIndex++;
        return this;
    }

    private DuckDBAppenderRow AppendValueInternalStruct<T>(T? value) where T : struct
    {
        CheckColumnAccess();

        if (value.HasValue)
        {
            vectorWriters[columnIndex].WriteValue(value.Value, rowIndex);
        }
        else
        {
            vectorWriters[columnIndex].WriteNull(rowIndex);
        }

        columnIndex++;
        return this;
    }

    private DuckDBAppenderRow AppendValueInternalClass<T>(T? value) where T : class
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
