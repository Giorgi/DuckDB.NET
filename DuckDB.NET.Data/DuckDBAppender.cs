using System;

#nullable enable
namespace DuckDB.NET.Data;

public class DuckDBAppender : IDisposable
{
    private bool closed;
    private readonly NET.DuckDBAppender nativeAppender;

    public DuckDBAppender(NET.DuckDBAppender appender)
    {
        nativeAppender = appender;
    }

    public DuckDBAppenderRow CreateRow()
    {
        // https://duckdb.org/docs/api/c/api#duckdb_appender_begin_row
        // Begin row is a no op. Do not need to call.
        // NativeMethods.Appender.DuckDBAppenderBeingRow(_connection.NativeConnection)

        if (closed)
        {
            throw new InvalidOperationException("Appender is already closed");
        }

        return new DuckDBAppenderRow(nativeAppender);
    }

    public void Close()
    {
        closed = true;

        try
        {
            var state = NativeMethods.Appender.DuckDBAppenderClose(nativeAppender);
            if (state == DuckDBState.DuckDBError)
            {
                ThrowLastError(nativeAppender);
            }
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

    internal static void ThrowLastError(NET.DuckDBAppender appender)
    {
        var errorMessage = NativeMethods.Appender.DuckDBAppenderError(appender).ToManagedString(false);

        throw new DuckDBException(errorMessage);
    }
}

public class DuckDBAppenderRow
{
    private readonly NET.DuckDBAppender appender;

    internal DuckDBAppenderRow(NET.DuckDBAppender appender)
    {
        this.appender = appender;
    }

    public void EndRow()
    {
        if (NativeMethods.Appender.DuckDBAppenderEndRow(appender) == DuckDBState.DuckDBError)
        {
            DuckDBAppender.ThrowLastError(appender);
        }
    }

    public DuckDBAppenderRow AppendValue(bool? value) => Append(value);

    public DuckDBAppenderRow AppendValue(string? value) => Append(value);

    public DuckDBAppenderRow AppendNullValue() => Append<object>(null);

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

    public DuckDBAppenderRow AppendValue(TimeOnly? nullable) => Append(nullable);
#endif

    public DuckDBAppenderRow AppendValue(DuckDBTime? value) => Append(value);

    #endregion

    private DuckDBAppenderRow Append<T>(T? value)
    {
        var state = value switch
        {
            null => NativeMethods.Appender.DuckDBAppendNull(appender),
            bool val => NativeMethods.Appender.DuckDBAppendBool(appender, val),
            string val => NativeMethods.Appender.DuckDBAppendVarchar(appender, val),

            sbyte val => NativeMethods.Appender.DuckDBAppendInt8(appender, val),
            short val => NativeMethods.Appender.DuckDBAppendInt16(appender, val),
            int val => NativeMethods.Appender.DuckDBAppendInt32(appender, val),
            long val => NativeMethods.Appender.DuckDBAppendInt64(appender, val),

            byte val => NativeMethods.Appender.DuckDBAppendUInt8(appender, val),
            ushort val => NativeMethods.Appender.DuckDBAppendUInt16(appender, val),
            uint val => NativeMethods.Appender.DuckDBAppendUInt32(appender, val),
            ulong val => NativeMethods.Appender.DuckDBAppendUInt64(appender, val),

            float val => NativeMethods.Appender.DuckDBAppendFloat(appender, val),
            double val => NativeMethods.Appender.DuckDBAppendDouble(appender, val),

            DateTime val => NativeMethods.Appender.DuckDBAppendTimestamp(appender, DuckDBTimestamp.FromDateTime(val)),
#if NET6_0_OR_GREATER
            DateOnly val => NativeMethods.Appender.DuckDBAppendDate(appender, NativeMethods.DateTime.DuckDBToDate(val)),
            TimeOnly val => NativeMethods.Appender.DuckDBAppendTime(appender, NativeMethods.DateTime.DuckDBToTime(val)),
#endif
            _ => throw new InvalidOperationException($"Unsupported type {typeof(T).Name}")
        };

        if (state == DuckDBState.DuckDBError)
        {
            DuckDBAppender.ThrowLastError(appender);
        }

        return this;
    }
}