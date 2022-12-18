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
	private readonly NET.DuckDBAppender _appender;

	internal DuckDBAppenderRow(NET.DuckDBAppender appender)
	{
		_appender = appender;
	}

	public void AppendValue(bool value)
	{
		NativeMethods.Appender.DuckDBAppendBool(_appender, value);
	}

	public void AppendValue(string value)
	{
		NativeMethods.Appender.DuckDBAppendVarchar(_appender, value);
	}

	#region Append Signed Int

	public void AppendValue(sbyte value)
	{
		NativeMethods.Appender.DuckDBAppendInt8(_appender, value);
	}
	
	public void AppendValue(short value)
	{
		NativeMethods.Appender.DuckDBAppendInt16(_appender, value);
	}
	
	public void AppendValue(int value)
	{
		NativeMethods.Appender.DuckDBAppendInt32(_appender, value);
	}
	
	public void AppendValue(long value)
	{
		NativeMethods.Appender.DuckDBAppendInt64(_appender, value);
	}

	#endregion

	#region Append Unsigned Int

	public void AppendValue(byte value)
	{
		NativeMethods.Appender.DuckDBAppendUInt8(_appender, value);
	}
	
	public void AppendValue(ushort value)
	{
		NativeMethods.Appender.DuckDBAppendUInt16(_appender, value);
	}
	
	public void AppendValue(uint value)
	{
		NativeMethods.Appender.DuckDBAppendUInt32(_appender, value);
	}
	
	public void AppendValue(ulong value)
	{
		NativeMethods.Appender.DuckDBAppendUInt64(_appender, value);
	}

	#endregion

	#region Append Float
	public void AppendValue(Single value)
	{
		NativeMethods.Appender.DuckDBAppendFloat(_appender, value);
	}
	public void AppendValue(Double value)
	{
		NativeMethods.Appender.DuckDBAppendDouble(_appender, value);
	}
	
	

	#endregion

	#region Append Temporal

	public void AppendValue(DateTime value)
	{
		NativeMethods.Appender.DuckDBAppendTimestamp(_appender, DuckDBTimestamp.FromDateTime(value));
	}
#if NET6_0_OR_GREATER
	public void AppendValue(DateOnly value)
	{
		DuckDBDateOnly date = value;
		AppendValue(date);
	}
#endif

	public void AppendValue(DuckDBDateOnly date)
	{
		AppendValue(NativeMethods.DateTime.DuckDBToDate(date));
	}
	
	public void AppendValue(DuckDBDate date)
	{
		NativeMethods.Appender.DuckDBAppendDate(_appender, date);
	}
	
#if NET6_0_OR_GREATER
	public void AppendValue(TimeOnly value)
	{
		DuckDBTimeOnly time = value;
		AppendValue(time);
	}
#endif
	
	public void AppendValue(DuckDBTimeOnly time)
	{
		NativeMethods.Appender.DuckDBAppendTime(_appender, NativeMethods.DateTime.DuckDBToTime(time));
	}
	
	public void AppendValue(DuckDBTime time)
	{
		NativeMethods.Appender.DuckDBAppendTime(_appender, time);
	}

	#endregion
	
	public void Dispose()
	{
		NativeMethods.Appender.DuckDBAppenderEndRow(_appender);
	}
}