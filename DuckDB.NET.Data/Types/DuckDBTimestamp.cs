using System;

namespace DuckDB.NET.Data.Types;

internal class DuckDBTimestamp : IDuckDBParameterValue
{
    private readonly DuckDBTimestampStruct nativeValue;

    private DuckDBTimestamp(DuckDBTimestampStruct native)
    {
        nativeValue = native;
    }
    
    public DuckDBState Bind(DuckDBPreparedStatement preparedStatement, long index)
    {
        return NativeMethods.PreparedStatements.DuckDBParamType(preparedStatement, index) switch
        {
            DuckDBType.DuckdbTypeDate => BindDate(preparedStatement, index),
            DuckDBType.DuckdbTypeTime => BindTime(preparedStatement, index),
            DuckDBType.DuckdbTypeTimestamp => BindTimestamp(preparedStatement, index),
            DuckDBType.DuckdbTypeInvalid => BindTimestamp(preparedStatement, index),
            _ => throw new ArgumentOutOfRangeException("Unexpected target data type.")
        };
    }

    private DuckDBState BindDate(DuckDBPreparedStatement preparedStatement, long index)
    {
        var date = NativeMethods.DateTime.DuckDBToDate(nativeValue.Date);
        return NativeMethods.PreparedStatements.DuckDBBindDate(preparedStatement, index, date);
    }
    
    private DuckDBState BindTime(DuckDBPreparedStatement preparedStatement, long index)
    {
        var time = NativeMethods.DateTime.DuckDBToTime(nativeValue.Time);
        return NativeMethods.PreparedStatements.DuckDBBindTime(preparedStatement, index, time);
    }
    
    private DuckDBState BindTimestamp(DuckDBPreparedStatement preparedStatement, long index)
    {
        var timestamp = NativeMethods.DateTime.DuckDBToTimestamp(nativeValue);
        return NativeMethods.PreparedStatements.DuckDBBindTimestamp(preparedStatement, index, timestamp);
    }

    public static DateTime Load(DuckDBResult result, long col, long row)
    {
        return (NativeMethods.Query.DuckDBColumnType(result, col) switch
        {
            DuckDBType.DuckdbTypeTimestamp => LoadTimestamp(result, col, row),
            DuckDBType.DuckdbTypeTime => LoadTime(result, col, row),
            DuckDBType.DuckdbTypeDate => LoadDate(result, col, row),
            _ => throw new ArgumentOutOfRangeException("Unexpected data type.")
        }).ToDateTime();
    }

    private static DuckDBTimestamp LoadTimestamp(DuckDBResult result, long col, long row)
    {
        var timestamp = NativeMethods.Types.DuckDbValueTimestamp(result, col, row);
        var timestampStruct = NativeMethods.DateTime.DuckDBFromTimestamp(timestamp);
        return new DuckDBTimestamp(timestampStruct);
    }
    
    private static DuckDBTimestamp LoadTime(DuckDBResult result, long col, long row)
    {
        var time = NativeMethods.Types.DuckDbValueTime(result, col, row);
        var timeStruct = NativeMethods.DateTime.DuckDBFromTime(time);
        var timestamp = new DuckDBTimestampStruct
        {
            Time = timeStruct,
            Date = new DuckDBDateStruct()
        };
        return new DuckDBTimestamp(timestamp);
    }
    
    private static DuckDBTimestamp LoadDate(DuckDBResult result, long col, long row)
    {
        var date = NativeMethods.Types.DuckDbValueDate(result, col, row);
        var dateStruct = NativeMethods.DateTime.DuckDBFromDate(date);
        var timestamp = new DuckDBTimestampStruct
        {
            Date = dateStruct,
            Time = new DuckDBTimeStruct()
        };
        return new DuckDBTimestamp(timestamp);
    }

    public DateTime ToDateTime()
    {
        return new DateTime(
            Math.Max(nativeValue.Date.Year, DateTime.MinValue.Year),
            Math.Max(nativeValue.Date.Month, DateTime.MinValue.Month),
            Math.Max(nativeValue.Date.Day, DateTime.MinValue.Day),
            Math.Max(nativeValue.Time.Hour, DateTime.MinValue.Hour),
            Math.Max(nativeValue.Time.Min, DateTime.MinValue.Minute),
            Math.Max(nativeValue.Time.Sec, DateTime.MinValue.Second),
            Math.Max(nativeValue.Time.Msec, DateTime.MinValue.Millisecond)
        );
    }

    public static DuckDBTimestamp FromDateTime(DateTime dateTime)
    {
        var nativeDate = new DuckDBDateStruct
        {
            Year = dateTime.Year,
            Month = (byte)dateTime.Month,
            Day = (byte)dateTime.Day
        };

        var nativeTime = new DuckDBTimeStruct
        {
            Hour = (byte)dateTime.Hour,
            Min = (byte)dateTime.Minute,
            Sec = (byte)dateTime.Second,
            Msec = dateTime.Millisecond
        };
        
        var value = new DuckDBTimestampStruct {
            Date = nativeDate,
            Time = nativeTime
        };
        return new DuckDBTimestamp(value);
    }
}