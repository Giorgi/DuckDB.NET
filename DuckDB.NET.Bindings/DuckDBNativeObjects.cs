using System;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Native;

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
    // duckdb_uhugeint
    UnsignedHugeInt,
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
public struct DuckDBTimeTzStruct
{
    public ulong Bits { get; set; }
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBTimeTz
{
    public DuckDBTime Time { get; set; }
    public int Offset { get; set; }
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBTimestampStruct
{
    public long Micros { get; set; }

    public readonly DateTime ToDateTime()
    {
        var ticks = Micros * 10 + Utils.UnixEpochTicks;
        return new DateTime(ticks);
    }
}

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBBlob : IDisposable
{
    public IntPtr Data { get; }

    public long Size { get; }

    public void Dispose()
    {
        NativeMethods.Helpers.DuckDBFree(Data);
    }
}

[StructLayout(LayoutKind.Sequential)]
public readonly struct DuckDBListEntry
{
    public ulong Offset { get; }
    public ulong Length { get; }
}

public struct DuckDBString
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