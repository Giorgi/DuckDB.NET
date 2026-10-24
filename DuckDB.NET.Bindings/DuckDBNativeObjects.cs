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
    // duckdb_bignum (named VARINT before DuckDB 1.4)
    VarInt = 35,
    // SQLNULL type
    SqlNull = 36,
    // string literal type, only useful as logical type
    StringLiteral = 37,
    // integer literal type, only useful as logical type
    IntegerLiteral = 38,
    // duckdb_time_ns
    TimeNs = 39,
    // GEOMETRY type, WKB blob
    Geometry = 40,
    // VARIANT type
    Variant = 41,
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBResult
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

    public void Close()
    {
        NativeMethods.Query.DuckDBDestroyResult(ref this);
    }
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBDate
{
    /// <summary>
    /// Represents DuckDB's positive infinity date value.
    /// This is the value used in the DuckDB source code for +infinity dates.
    /// </summary>
    public static readonly DuckDBDate PositiveInfinity = new() { Days = int.MaxValue };
    /// <summary>
    /// Represents DuckDB's negative infinity date value.
    /// This is the value used in the DuckDB source code for -infinity dates.
    /// </summary>
    public static readonly DuckDBDate NegativeInfinity = new() { Days = -int.MaxValue };

    public int Days { get; set; }

    public bool IsInfinity => IsPositiveInfinity || IsNegativeInfinity;
    public bool IsPositiveInfinity => Days == int.MaxValue;
    public bool IsNegativeInfinity => Days == -int.MaxValue;
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
public struct DuckDBTimeTz : IEquatable<DuckDBTimeTz>
{
    public DuckDBTimeOnly Time { get; set; }
    public int Offset { get; set; }

    // Without these, Equals is ValueType.Equals(object), which boxes and compares the fields through reflection.
    public readonly bool Equals(DuckDBTimeTz other) => Time.Equals(other.Time) && Offset == other.Offset;

    public readonly override bool Equals(object? obj) => obj is DuckDBTimeTz other && Equals(other);

    public readonly override int GetHashCode() => HashCode.Combine(Time, Offset);
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBTimestampStruct
{
    /// <summary>
    /// Represents DuckDB's positive infinity timestamp value.
    /// This is the value used in the DuckDB source code for +infinity timestamps.
    /// </summary>
    public static readonly DuckDBTimestampStruct PositiveInfinity = new() { Micros = long.MaxValue };
    /// <summary>
    /// Represents DuckDB's negative infinity timestamp value.
    /// This is the value used in the DuckDB source code for -infinity timestamps.
    /// </summary>
    public static readonly DuckDBTimestampStruct NegativeInfinity = new() { Micros = -long.MaxValue };

    public long Micros { get; set; }

    public bool IsInfinity => IsPositiveInfinity || IsNegativeInfinity;
    public bool IsPositiveInfinity => Micros == long.MaxValue;
    public bool IsNegativeInfinity => Micros == -long.MaxValue;
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
public readonly struct DuckDBListEntry(ulong offset, ulong length)
{
    public ulong Offset { get; } = offset;
    public ulong Length { get; } = length;
}

/// <summary>
/// DuckDB's 16-byte string value (duckdb_string_t). A string of up to 12 bytes is kept inside the value; a longer
/// one is pointed to. Both start with the length. Read it in place, through a pointer into the vector: for a short
/// string, <see cref="Data"/> points into the value itself, so on a copy it is valid only while the copy lives.
/// </summary>
[StructLayout(LayoutKind.Explicit)]
public struct DuckDBString
{
    // The union of duckdb_string_t: both halves start at the same byte.
    [FieldOffset(0)]
    private DuckDBStringPointer pointer;

    [FieldOffset(0)]
    private DuckDBStringInlined inlined;

    private const int InlineStringMaxLength = 12;

    /// <summary>
    /// Builds a string that is kept inside the value: the UTF-8 bytes of <paramref name="value"/>, then zeros.
    /// Returns false, with a default value, when they take more than 12 bytes, or when <paramref name="value"/>
    /// contains a NUL character, because DuckDB.NET ends a string at its first NUL (DuckDB itself would keep it).
    /// Write such a string through DuckDB, with its length.
    /// </summary>
    public static unsafe bool TryCreateInlined(ReadOnlySpan<char> value, out DuckDBString result)
    {
        // Starts as zeros: DuckDB compares and hashes a short string by its length and all 12 bytes, so the bytes
        // after the string have to be zero.
        result = default;

        if (value.Length > InlineStringMaxLength)
        {
            return false;
        }

        fixed (sbyte* data = result.inlined.Data)
        {
            var bytes = new Span<byte>(data, InlineStringMaxLength);

            // Characters of more than one byte, more than 12 bytes together; or a NUL character (UTF-8 has a zero
            // byte only for it), which DuckDB.NET ends a string at.
            if (!Encoding.UTF8.TryGetBytes(value, bytes, out var length) || bytes[..length].Contains((byte)0))
            {
                result = default;
                return false;
            }

            result.inlined.Length = (uint)length;
        }

        return true;
    }

    public readonly int Length => (int)inlined.Length;

    public readonly unsafe sbyte* Data
    {
        get
        {
            if (Length <= InlineStringMaxLength)
            {
                fixed (sbyte* pointerToFirst = inlined.Data)
                {
                    return pointerToFirst;
                }
            }
            else
            {
                return pointer.Data;
            }
        }
    }
}

// A DuckDBString of up to 12 bytes, kept inside the value: the length, then the bytes. The bytes after the string
// must be zero, because DuckDB compares and hashes a short string by its 12 bytes whole; TryCreateInlined makes sure.
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct DuckDBStringInlined
{
    public uint Length;

    public fixed sbyte Data[12];
}

// A DuckDBString of more than 12 bytes: the length, the first 4 bytes, and a pointer to the whole string.
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct DuckDBStringPointer
{
    public uint Length;

    public fixed sbyte Prefix[4];

    public sbyte* Data;
}

[StructLayout(LayoutKind.Sequential)]
public struct DuckDBQueryProgress
{
    public double Percentage { get; }
    public ulong RowsProcessed { get; }
    public ulong TotalRowsToProcess { get; }
}