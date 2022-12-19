using System;
#nullable enable
namespace DuckDB.NET.Data;

public class DuckDBAppender : IDisposable
{
	private readonly DuckDBConnection _connection;
	private readonly string _table;
	private readonly NET.DuckDBAppender _nativeAppender;

	internal DuckDBAppender(DuckDBConnection connection, string table)
	{
		_connection = connection;
		_table = table;

		NativeMethods.Appender.DuckDBAppenderCreate(connection.NativeConnection, null, table, out _nativeAppender);
	}


	public DuckDBAppenderRow CreateRow()
	{
		// https://duckdb.org/docs/api/c/api#duckdb_appender_begin_row
		// Begin row is a no op. Do not need to call.
		// NativeMethods.Appender.DuckDBAppenderBeingRow(_connection.NativeConnection)

		return new DuckDBAppenderRow(_nativeAppender);
	}
	
	private void ReleaseUnmanagedResources()
	{
		NativeMethods.Appender.DuckDBAppenderFlush(_nativeAppender);
		NativeMethods.Appender.DuckDBAppenderClose(_nativeAppender);
	}

	private void Dispose(bool disposing)
	{
		ReleaseUnmanagedResources();
	}

	public void Dispose()
	{
		Dispose(true);
		GC.SuppressFinalize(this);
	}

	~DuckDBAppender()
	{
		Dispose(false);
	}
}

public class DuckDBAppenderRow : IDisposable
{
	private readonly NET.DuckDBAppender appender;

	internal DuckDBAppenderRow(NET.DuckDBAppender appender)
	{
		this.appender = appender;
	}

	public DuckDBAppenderRow AppendValue(bool value)
	{
		NativeMethods.Appender.DuckDBAppendBool(appender, value);
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(bool? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}


	public DuckDBAppenderRow AppendValue(string value)
	{
		if (value is {} str)
			NativeMethods.Appender.DuckDBAppendVarchar(appender, value);
		else 
			NativeMethods.Appender.DuckDBAppendNull(appender);

		return this;
	}

	public DuckDBAppenderRow AppendNullValue()
	{
		NativeMethods.Appender.DuckDBAppendNull(appender);
		return this;
	}

	#region Append Signed Int

	public DuckDBAppenderRow AppendValue(sbyte value)
	{
		NativeMethods.Appender.DuckDBAppendInt8(appender, value);
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(short value)
	{
		NativeMethods.Appender.DuckDBAppendInt16(appender, value);
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(int value)
	{
		NativeMethods.Appender.DuckDBAppendInt32(appender, value);
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(long value)
	{
		NativeMethods.Appender.DuckDBAppendInt64(appender, value);
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(sbyte? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}
	
	public DuckDBAppenderRow AppendValue(short? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}
	
	public DuckDBAppenderRow AppendValue(int? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}
	
	public DuckDBAppenderRow AppendValue(long? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}

	#endregion

	#region Append Unsigned Int

	public DuckDBAppenderRow AppendValue(byte value)
	{
		NativeMethods.Appender.DuckDBAppendUInt8(appender, value);
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(ushort value)
	{
		NativeMethods.Appender.DuckDBAppendUInt16(appender, value);
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(uint value)
	{
		NativeMethods.Appender.DuckDBAppendUInt32(appender, value);
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(ulong value)
	{
		NativeMethods.Appender.DuckDBAppendUInt64(appender, value);
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(byte? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}
	
	public DuckDBAppenderRow AppendValue(ushort? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}
	
	public DuckDBAppenderRow AppendValue(uint? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}
	
	public DuckDBAppenderRow AppendValue(ulong? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}

	#endregion

	#region Append Float
	public DuckDBAppenderRow AppendValue(Single value)
	{
		NativeMethods.Appender.DuckDBAppendFloat(appender, value);
		return this;
	}
	public DuckDBAppenderRow AppendValue(Double value)
	{
		NativeMethods.Appender.DuckDBAppendDouble(appender, value);
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(Single? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}
	
	public DuckDBAppenderRow AppendValue(Double? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}

	#endregion

	#region Append Temporal
#if NET6_0_OR_GREATER
	public DuckDBAppenderRow AppendValue(DateOnly value)
	{
		DuckDBDateOnly date = value;
		return AppendValue(date);
	}
	
	public DuckDBAppenderRow AppendValue(TimeOnly value)
	{
		DuckDBTimeOnly time = value;
		return AppendValue(time);
	}
	
	public DuckDBAppenderRow AppendValue(DateOnly? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}
	
	public DuckDBAppenderRow AppendValue(TimeOnly? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}
#endif

	public DuckDBAppenderRow AppendValue(DateTime value)
	{
		NativeMethods.Appender.DuckDBAppendTimestamp(appender, DuckDBTimestamp.FromDateTime(value));
		return this;
	}

	public DuckDBAppenderRow AppendValue(DuckDBDateOnly date)
	{
		AppendValue(NativeMethods.DateTime.DuckDBToDate(date));
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(DuckDBDate date)
	{
		NativeMethods.Appender.DuckDBAppendDate(appender, date);
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(DuckDBTimeOnly time)
	{
		NativeMethods.Appender.DuckDBAppendTime(appender, NativeMethods.DateTime.DuckDBToTime(time));
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(DuckDBTime time)
	{
		NativeMethods.Appender.DuckDBAppendTime(appender, time);
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(DateTime? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}
	public DuckDBAppenderRow AppendValue(DuckDBDateOnly? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}
	public DuckDBAppenderRow AppendValue(DuckDBDate? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}
	public DuckDBAppenderRow AppendValue(DuckDBTimeOnly? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}
	public DuckDBAppenderRow AppendValue(DuckDBTime? nullable)
	{
		if (nullable is { } value)
			return this.AppendValue(value);
		return this.AppendNullValue();
	}


	#endregion
	
	public void Dispose()
	{
		NativeMethods.Appender.DuckDBAppenderEndRow(appender);
	}
}