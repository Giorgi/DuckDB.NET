using System;
using System.Runtime.InteropServices;

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

		if (NativeMethods.Appender.DuckDBAppenderCreate(connection.NativeConnection, null, table, out _nativeAppender) == DuckDBState.DuckDBError) 
			DuckDBAppenderRow.ThrowLastError(_nativeAppender);
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
		if (NativeMethods.Appender.DuckDBAppenderFlush(_nativeAppender) == DuckDBState.DuckDBError) 
			DuckDBAppenderRow.ThrowLastError(_nativeAppender);
		if (NativeMethods.Appender.DuckDBAppenderClose(_nativeAppender) == DuckDBState.DuckDBError) 
			DuckDBAppenderRow.ThrowLastError(_nativeAppender);
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
		if (NativeMethods.Appender.DuckDBAppendBool(appender, value) == DuckDBState.DuckDBError) 
			ThrowLastError();
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
		{
			if (NativeMethods.Appender.DuckDBAppendVarchar(appender, value) == DuckDBState.DuckDBError)
				ThrowLastError();
		}
		else
		{
			if (NativeMethods.Appender.DuckDBAppendNull(appender) == DuckDBState.DuckDBError)
				ThrowLastError();
		}

		return this;
	}

	public DuckDBAppenderRow AppendNullValue()
	{
		if (NativeMethods.Appender.DuckDBAppendNull(appender) == DuckDBState.DuckDBError) 
			ThrowLastError();
		return this;
	}

	#region Append Signed Int

	public DuckDBAppenderRow AppendValue(sbyte value)
	{
		if (NativeMethods.Appender.DuckDBAppendInt8(appender, value) == DuckDBState.DuckDBError)
			ThrowLastError();
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(short value)
	{
		if (NativeMethods.Appender.DuckDBAppendInt16(appender, value) == DuckDBState.DuckDBError)
			ThrowLastError();
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(int value)
	{
		if (NativeMethods.Appender.DuckDBAppendInt32(appender, value) == DuckDBState.DuckDBError)
			ThrowLastError();
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(long value)
	{
		if (NativeMethods.Appender.DuckDBAppendInt64(appender, value) == DuckDBState.DuckDBError)
			ThrowLastError();
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
		if (NativeMethods.Appender.DuckDBAppendUInt8(appender, value) == DuckDBState.DuckDBError) 
			ThrowLastError();
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(ushort value)
	{
		if (NativeMethods.Appender.DuckDBAppendUInt16(appender, value) == DuckDBState.DuckDBError)
			ThrowLastError();
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(uint value)
	{
		if (NativeMethods.Appender.DuckDBAppendUInt32(appender, value) == DuckDBState.DuckDBError)
			ThrowLastError();
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(ulong value)
	{
		if (NativeMethods.Appender.DuckDBAppendUInt64(appender, value) == DuckDBState.DuckDBError)
			ThrowLastError();
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
		if (NativeMethods.Appender.DuckDBAppendFloat(appender, value) == DuckDBState.DuckDBError) 
			ThrowLastError();
		return this;
	}
	public DuckDBAppenderRow AppendValue(Double value)
	{
		if (NativeMethods.Appender.DuckDBAppendDouble(appender, value) == DuckDBState.DuckDBError)
			ThrowLastError();
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
		if (NativeMethods.Appender.DuckDBAppendTimestamp(appender, DuckDBTimestamp.FromDateTime(value)) == DuckDBState.DuckDBError) 
			ThrowLastError();
		return this;
	}

	public DuckDBAppenderRow AppendValue(DuckDBDateOnly date)
	{
		AppendValue(NativeMethods.DateTime.DuckDBToDate(date));
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(DuckDBDate date)
	{
		if (NativeMethods.Appender.DuckDBAppendDate(appender, date) == DuckDBState.DuckDBError) 
			ThrowLastError();
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(DuckDBTimeOnly time)
	{
		if (NativeMethods.Appender.DuckDBAppendTime(appender, NativeMethods.DateTime.DuckDBToTime(time)) == DuckDBState.DuckDBError) 
			ThrowLastError();
		return this;
	}
	
	public DuckDBAppenderRow AppendValue(DuckDBTime time)
	{
		if (NativeMethods.Appender.DuckDBAppendTime(appender, time) == DuckDBState.DuckDBError) 
			ThrowLastError();
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

	private void ThrowLastError()
	{
		ThrowLastError(appender);
	}
	
	internal static void ThrowLastError(NET.DuckDBAppender appender)
	{
		var errorMessagePtr = NativeMethods.Appender.DuckDBAppenderError(appender);
		if (errorMessagePtr == IntPtr.Zero) return;

		if (Marshal.PtrToStringAnsi(errorMessagePtr) is { } errorMessage)
			throw new DuckDBException(errorMessage);
	}


	#endregion
	
	public void Dispose()
	{
		if (NativeMethods.Appender.DuckDBAppenderEndRow(appender) == DuckDBState.DuckDBError)
			ThrowLastError();
	}
}