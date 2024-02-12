using System;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.InteropServices;

namespace DuckDB.NET;

public enum DuckDBState
{
    Success = 0,
    Error = 1
}

public enum DuckDBType
{
    Invalid = 0,
    // bool
    Boolean,
    // int8_t
    TinyInt,
    // int16_t
    SmallInt,
    // int32_t
    Integer,
    // int64_t
    BigInt,
    // uint8_t
    UnsignedTinyInt,
    // uint16_t
    UnsignedSmallInt,
    // uint32_t
    UnsignedInteger,
    // uint64_t
    UnsignedBigInt,
    // float
    Float,
    // double
    Double,
    // duckdb_timestamp
    Timestamp,
    // duckdb_date
    Date,
    // duckdb_time
    Time,
    // duckdb_interval
    Interval,
    // duckdb_hugeint
    HugeInt,
    // const char*
    Varchar,
    // duckdb_blob
    Blob,
    //decimal
    Decimal,
    // duckdb_timestamp, in seconds
    TimestampS,
    // duckdb_timestamp, in milliseconds
    TimestampMs,
    // duckdb_timestamp, in nanoseconds
    TimestampNs,
    // enum type, only useful as logical type
    Enum,
    // list type, only useful as logical type
    List,
    // struct type, only useful as logical type
    Struct,
    // map type, only useful as logical type
    Map,
    // duckdb_hugeint
    Uuid,
    // union type, only useful as logical type
    Union,
    // duckdb_bit
    Bit,
    // duckdb_time_tz
    TimeTz,
    // duckdb_timestamp
    TimestampTz
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBResult : IDisposable
{
    [Obsolete]
    private long ColumnCount;

    [Obsolete]
    private long RowCount;

    [Obsolete]
    private long RowsChanged;

    [Obsolete]
    private IntPtr columns;

    [Obsolete]
    private IntPtr ErrorMessage;

    private IntPtr internal_data;

    public void Dispose()
    {
        NativeMethods.Query.DuckDBDestroyResult(ref this);
    }
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBDate
{
    public int Days { get; set; }
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBTime
{
    public long Micros { get; set; }
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBTimestampStruct
{
    public long Micros { get; set; }

    public DateTime ToDateTime()
    {
        var ticks = Micros * 10 + Utils.UnixEpochTicks;
        return new DateTime(ticks);
    }
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBBlob : IDisposable
{
    public IntPtr Data { get; }

    public long Size { get; }

    public void Dispose()
    {
        NativeMethods.Helpers.DuckDBFree(Data);
    }
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBListEntry
{
    public ulong Offset { get; }
    public ulong Length { get; }
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBHugeInt
{
    private static readonly BigInteger Base = BigInteger.Pow(2, 64);

    public static BigInteger HugeIntMinValue { get; } = BigInteger.Parse("-170141183460469231731687303715884105727");
    public static BigInteger HugeIntMaxValue { get; } = BigInteger.Parse("170141183460469231731687303715884105727");

    public DuckDBHugeInt(BigInteger value)
    {
        if (value < HugeIntMinValue || value > HugeIntMaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(value), $"value must be between {HugeIntMinValue} and {HugeIntMaxValue}");
        }

        var upper = (long)BigInteger.DivRem(value, Base, out var rem);

        if (rem < 0)
        {
            rem += Base;
            upper -= 1;
        }

        Upper = upper;
        Lower = (ulong)rem;
    }

    public DuckDBHugeInt(ulong lower, long upper)
    {
        Lower = lower;
        Upper = upper;
    }

    public ulong Lower { get; }
    public long Upper { get; }

    public BigInteger ToBigInteger()
    {
        return Upper * BigInteger.Pow(2, 64) + Lower;
    }
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBDecimal
{
    public byte Width { get; }
    public byte Scale { get; }

    public DuckDBHugeInt Value { get; }
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBInterval
{
    public int Months { get; }
    public int Days { get; }
    public ulong Micros { get; }

    public DuckDBInterval(int months, int days, ulong micros)
        => (Months, Days, Micros) = (months, days, micros);

    public static explicit operator TimeSpan(DuckDBInterval interval)
    {
        (var timeSpan, var exception) = ToTimeSpan(interval);
        return timeSpan ?? throw exception!;
    }
    public static implicit operator DuckDBInterval(TimeSpan timeSpan) => FromTimeSpan(timeSpan);

#if NET6_0_OR_GREATER
    public bool TryConvert([NotNullWhen(true)] out TimeSpan? timeSpan)
#else
    public bool TryConvert(out TimeSpan? timeSpan)
#endif
    {
        (timeSpan, var exception) = ToTimeSpan(this);
        return exception is null;
    }

    private static (TimeSpan?, Exception?) ToTimeSpan(DuckDBInterval interval)
    {
        if (interval.Months > 0)
        {
            return (null, new ArgumentOutOfRangeException(nameof(interval), $"Cannot convert a value of type {nameof(DuckDBInterval)} to type {nameof(TimeSpan)} when the attribute 'Months' is greater or equal to 1"));
        }

        var millisecondsByDay = (ulong)(24 * 60 * 60 * 1e6);
        int days = 0;
        ulong micros = interval.Micros;

        if (interval.Micros >= millisecondsByDay)
        {
            days = Convert.ToInt32(Math.Floor((double)(interval.Micros / millisecondsByDay)));
            if (days > int.MaxValue - interval.Days)
            {
                return (null, new ArgumentOutOfRangeException(nameof(interval), $"Cannot convert a value of type {nameof(DuckDBInterval)} to type {nameof(TimeSpan)} when the total days value is larger than {int.MaxValue}"));
            }

            if (days > 0)
            {
                micros = interval.Micros - ((ulong)days * millisecondsByDay);
            }
            days = interval.Days + days;
        }
        else
            days = interval.Days;

        if (micros * 10 > long.MaxValue)
        {
            return (null, new ArgumentOutOfRangeException(nameof(interval), $"Cannot convert a value of type {nameof(DuckDBInterval)} to type {nameof(TimeSpan)} when the value of microseconds is larger than {long.MaxValue / 10}"));
        }

        if ((ulong)days * millisecondsByDay * 100 + micros * 10 > long.MaxValue)
        {
            return (null, new ArgumentOutOfRangeException(nameof(interval), $"Cannot convert a value of type {nameof(DuckDBInterval)} to type {nameof(TimeSpan)} when the value of total microseconds is larger than {long.MaxValue}"));
        }

        return (new TimeSpan(days, 0, 0, 0) + new TimeSpan((long)micros * 10), null);
    }

    private static DuckDBInterval FromTimeSpan(TimeSpan timeSpan)
        => new(
            0
            , timeSpan.Days
            , Convert.ToUInt64(timeSpan.Ticks / 10 - new TimeSpan(timeSpan.Days, 0, 0, 0).Ticks / 10)
        );
}

public partial struct DuckDBString
{
    public _value_e__Union value;

    private const int InlineStringMaxLength = 12;

    public readonly int Length => (int)value.inlined.length;

    public readonly unsafe sbyte* Data
    {
        get
        {
            if (Length <= InlineStringMaxLength)
            {
                fixed (sbyte* pointerToFirst = value.inlined.inlined)
                {
                    return pointerToFirst;
                }
            }
            else
            {
                return value.pointer.ptr;
            }
        }
    }

    [StructLayout(LayoutKind.Explicit)]
    public partial struct _value_e__Union
    {
        [FieldOffset(0)]
        public DuckDBStringPointer pointer;

        [FieldOffset(0)]
        public DuckDBStringInlined inlined;

        public unsafe partial struct DuckDBStringPointer
        {
            public uint length;

            public fixed sbyte prefix[4];

            public sbyte* ptr;
        }

        public unsafe partial struct DuckDBStringInlined
        {
            public uint length;

            public fixed sbyte inlined[12];
        }
    }
}

public enum DuckDBStatementType
{
    Invalid = 0,
    Select,
    Insert,
    Update,
    Explain,
    Delete,
    Prepare,
    Create,
    Execute,
    Alter,
    Transaction,
    Copy,
    Analyze,
    VariableSet,
    CreateFunc,
    Drop,
    Export,
    Pragma,
    Show,
    Vacuum,
    Call,
    Set,
    Load,
    Relation,
    Extension,
    LogicalPlan,
    Attach,
    Detach,
    Multi,
}

public enum DuckDBResultType
{
    Invalid = 0,
    ChangedRows,
    Nothing,
    QueryResult,
}