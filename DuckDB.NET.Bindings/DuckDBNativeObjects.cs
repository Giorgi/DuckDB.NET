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
    Boolean = 1,
    // int8_t
    TinyInt = 2,
    // int16_t
    SmallInt = 3,
    // int32_t
    Integer = 4,
    // int64_t
    BigInt = 5,
    // uint8_t
    UnsignedTinyInt = 6,
    // uint16_t
    UnsignedSmallInt = 7,
    // uint32_t
    UnsignedInteger = 8,
    // uint64_t
    UnsignedBigInt = 9,
    // float
    Float = 10,
    // double
    Double = 11,
    // duckdb_timestamp
    Timestamp = 12,
    // duckdb_date
    Date = 13,
    // duckdb_time
    Time = 14,
    // duckdb_interval
    Interval = 15,
    // duckdb_hugeint
    HugeInt = 16,
    // duckdb_uhugeint
    UnsignedHugeInt = 32,
    // const char*
    Varchar = 17,
    // duckdb_blob
    Blob = 18,
    //decimal
    Decimal = 19,
    // duckdb_timestamp, in seconds
    TimestampS = 20,
    // duckdb_timestamp, in milliseconds
    TimestampMs = 21,
    // duckdb_timestamp, in nanoseconds
    TimestampNs = 22,
    // enum type, only useful as logical type
    Enum = 23,
    // list type, only useful as logical type
    List = 24,
    // duckdb_array, only useful as logical type
    Array = 33,
    // struct type, only useful as logical type
    Struct = 25,
    // map type, only useful as logical type
    Map = 26,
    // duckdb_hugeint
    Uuid = 27,
    // union type, only useful as logical type
    Union = 28,
    // duckdb_bit
    Bit = 29,
    // duckdb_time_tz
    TimeTz = 30,
    // duckdb_timestamp
    TimestampTz = 31,
    // ANY type
    Any = 34,
    // duckdb_varint
    VarInt = 35,
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
    public DuckDBTimeOnly Time { get; set; }
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
public struct DuckDBListEntry(ulong offset, ulong length)
{
    public ulong Offset { get; private set; } = offset;
    public ulong Length { get; private set; } = length;
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

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBQueryProgressType
{
    public double Percentage { get; }
    public ulong RowsProcessed { get; }
    public ulong TotalRowsToProcess { get; }
}