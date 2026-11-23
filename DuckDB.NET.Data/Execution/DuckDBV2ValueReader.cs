using System.Globalization;
using System.Numerics;
using System.Runtime.CompilerServices;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Execution;

/// <summary>
/// A parameter of a table function as the function's bind callback reads it: a V2 value, read with the conversions
/// of V1's <see cref="DuckDBValue.GetValue{T}"/>, case for case. A value is produced as its own .NET type and refused as
/// any other, except <c>object</c>, a nullable or an interface of it; HUGEINT and UHUGEINT are also
/// <see cref="Int128"/> and <see cref="UInt128"/>; a timestamp is also a <see cref="DateTimeOffset"/> at UTC; DATE and
/// TIME are also the provider's own types.
/// </summary>
internal sealed unsafe class DuckDBV2ValueReader : IDuckDBValueReader, IDisposable
{
    private readonly DuckDBV2Value value;
    private readonly DUCKDB_V2_LOGICAL_TYPE_ID typeId;
    private readonly bool isNull;

    public DuckDBV2ValueReader(DuckDBV2Value value)
    {
        this.value = value;

        using var lease = value.Acquire();
        _duckdb_v2_error_info* error = null;

        bool valueIsNull;
        NativeMethodsV2.duckdb_v2_value_is_null(lease.Pointer, &valueIsNull, &error).ThrowIfFailed(ref error);
        isNull = valueIsNull;

        _duckdb_v2_logical_type* type;
        NativeMethodsV2.duckdb_v2_value_get_logical_type(lease.Pointer, &type, &error).ThrowIfFailed(ref error);
        using var owned = new DuckDBV2LogicalType(type);
        using var typeLease = owned.Acquire();
        typeId = DuckDBV2LogicalTypes.GetId(typeLease.Pointer);
    }

    public bool IsNull() => isNull;

    public T GetValue<T>()
    {
        using var lease = value.Acquire();
        var pointer = lease.Pointer;

        return typeId switch
        {
            DUCKDB_V2_LOGICAL_TYPE_ID.BOOLEAN => Cast<T, bool>(Get<bool>(pointer, &NativeMethodsV2.duckdb_v2_value_get_bool)),

            DUCKDB_V2_LOGICAL_TYPE_ID.TINYINT => Cast<T, sbyte>(Get<sbyte>(pointer, &NativeMethodsV2.duckdb_v2_value_get_tinyint)),
            DUCKDB_V2_LOGICAL_TYPE_ID.SMALLINT => Cast<T, short>(Get<short>(pointer, &NativeMethodsV2.duckdb_v2_value_get_smallint)),
            DUCKDB_V2_LOGICAL_TYPE_ID.INTEGER => Cast<T, int>(Get<int>(pointer, &NativeMethodsV2.duckdb_v2_value_get_int)),
            DUCKDB_V2_LOGICAL_TYPE_ID.BIGINT => Cast<T, long>(Get<long>(pointer, &NativeMethodsV2.duckdb_v2_value_get_bigint)),

            DUCKDB_V2_LOGICAL_TYPE_ID.UTINYINT => Cast<T, byte>(Get<byte>(pointer, &NativeMethodsV2.duckdb_v2_value_get_utinyint)),
            DUCKDB_V2_LOGICAL_TYPE_ID.USMALLINT => Cast<T, ushort>(Get<ushort>(pointer, &NativeMethodsV2.duckdb_v2_value_get_usmallint)),
            DUCKDB_V2_LOGICAL_TYPE_ID.UINTEGER => Cast<T, uint>(Get<uint>(pointer, &NativeMethodsV2.duckdb_v2_value_get_uint)),
            DUCKDB_V2_LOGICAL_TYPE_ID.UBIGINT => Cast<T, ulong>(Get<ulong>(pointer, &NativeMethodsV2.duckdb_v2_value_get_ubigint)),

            DUCKDB_V2_LOGICAL_TYPE_ID.FLOAT => Cast<T, float>(Get<float>(pointer, &NativeMethodsV2.duckdb_v2_value_get_float)),
            DUCKDB_V2_LOGICAL_TYPE_ID.DOUBLE => Cast<T, double>(Get<double>(pointer, &NativeMethodsV2.duckdb_v2_value_get_double)),

            // V1 parsed the text of the value; this is the same number, from its scaled integer.
            DUCKDB_V2_LOGICAL_TYPE_ID.DECIMAL => Cast<T, decimal>(GetDecimal(pointer)),

            DUCKDB_V2_LOGICAL_TYPE_ID.UUID => Cast<T, Guid>(GetHugeInt(pointer, &NativeMethodsV2.duckdb_v2_value_get_uuid).ConvertToGuid()),

            // An Int128 or a UInt128 is produced as itself; any other type gets the BigInteger.
            DUCKDB_V2_LOGICAL_TYPE_ID.HUGEINT when typeof(T) == typeof(Int128) => Cast<T, Int128>(GetHugeInt(pointer, &NativeMethodsV2.duckdb_v2_value_get_hugeint).ToInt128()),
            DUCKDB_V2_LOGICAL_TYPE_ID.HUGEINT => Cast<T, BigInteger>(GetHugeInt(pointer, &NativeMethodsV2.duckdb_v2_value_get_hugeint).ToBigInteger()),
            DUCKDB_V2_LOGICAL_TYPE_ID.UHUGEINT when typeof(T) == typeof(UInt128) => Cast<T, UInt128>(GetUHugeInt(pointer).ToUInt128()),
            DUCKDB_V2_LOGICAL_TYPE_ID.UHUGEINT => Cast<T, BigInteger>(GetUHugeInt(pointer).ToBigInteger()),

            DUCKDB_V2_LOGICAL_TYPE_ID.VARCHAR => Cast<T, string>(GetVarchar(pointer)),

            // DATE and TIME can be read as the .NET types or as the provider's own types.
            DUCKDB_V2_LOGICAL_TYPE_ID.DATE when typeof(T) == typeof(DuckDBDateOnly) => Cast<T, DuckDBDateOnly>(GetDate(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.DATE => Cast<T, DateOnly>((DateOnly)GetDate(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIME when typeof(T) == typeof(DuckDBTimeOnly) => Cast<T, DuckDBTimeOnly>(GetTime(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIME => Cast<T, TimeOnly>((TimeOnly)GetTime(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIME_NS when typeof(T) == typeof(DuckDBTimeOnly) => Cast<T, DuckDBTimeOnly>(GetTimeNs(pointer).ToDuckDBTimeOnly()),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIME_NS => Cast<T, TimeOnly>(new TimeOnly(GetTimeNs(pointer).Ticks)),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIME_TZ => Cast<T, DateTimeOffset>(GetTimeTz(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.INTERVAL => Cast<T, TimeSpan>((TimeSpan)GetInterval(pointer)),

            DUCKDB_V2_LOGICAL_TYPE_ID.TIMESTAMP => CastTimestamp<T>(GetTimestamp(Get<long>(pointer, &NativeMethodsV2.duckdb_v2_value_get_timestamp), 1, 0)),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIMESTAMP_SEC => CastTimestamp<T>(GetTimestamp(Get<long>(pointer, &NativeMethodsV2.duckdb_v2_value_get_timestamp_sec), 1_000_000, 0)),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIMESTAMP_MS => CastTimestamp<T>(GetTimestamp(Get<long>(pointer, &NativeMethodsV2.duckdb_v2_value_get_timestamp_ms), 1_000, 0)),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIMESTAMP_NS => CastTimestamp<T>(GetTimestampNs(Get<long>(pointer, &NativeMethodsV2.duckdb_v2_value_get_timestamp_ns))),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIMESTAMP_TZ => CastTimestamp<T>(GetTimestamp(Get<long>(pointer, &NativeMethodsV2.duckdb_v2_value_get_timestamp_tz), 1, 0)),

            _ => throw new NotImplementedException($"Cannot read value of type {typeof(T).FullName}")
        };
    }

    // The value as it was produced, or as object, a nullable or an interface of it. Any other type is refused.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T Cast<T, TSource>(TSource source) => typeof(T) == typeof(TSource)
        ? Unsafe.As<TSource, T>(ref source)
        : source is T result ? result : throw CannotCast(typeof(TSource), typeof(T));

    // A timestamp is a DateTime, or the instant at UTC when it is read as DateTimeOffset, as the column reader gives it.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T CastTimestamp<T>(DateTime timestamp) => typeof(T) == typeof(DateTimeOffset) || typeof(T) == typeof(DateTimeOffset?)
        ? Cast<T, DateTimeOffset>(new DateTimeOffset(timestamp, TimeSpan.Zero))
        : Cast<T, DateTime>(timestamp);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static InvalidCastException CannotCast(Type from, Type to) => new($"Cannot read a value of type {from.Name} as {to.Name}.");

    private static TValue Get<TValue>(_duckdb_v2_value* pointer, delegate* managed<_duckdb_v2_value*, TValue*, _duckdb_v2_error_info**, DUCKDB_V2_ERROR> getter)
        where TValue : unmanaged
    {
        _duckdb_v2_error_info* error = null;
        TValue result;
        getter(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static DuckDBHugeInt GetHugeInt(_duckdb_v2_value* pointer, delegate* managed<_duckdb_v2_value*, duckdb_v2_hugeint_t*, _duckdb_v2_error_info**, DUCKDB_V2_ERROR> getter)
    {
        var hugeInt = Get(pointer, getter);
        return new DuckDBHugeInt(hugeInt.lower, hugeInt.upper);
    }

    private static DuckDBUHugeInt GetUHugeInt(_duckdb_v2_value* pointer)
    {
        var hugeInt = Get<duckdb_v2_uhugeint_t>(pointer, &NativeMethodsV2.duckdb_v2_value_get_uhugeint);
        return new DuckDBUHugeInt(hugeInt.lower, hugeInt.upper);
    }

    private static decimal GetDecimal(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        duckdb_v2_hugeint_t scaled;
        byte width, scale;
        NativeMethodsV2.duckdb_v2_value_get_decimal(pointer, &scaled, &width, &scale, &error).ThrowIfFailed(ref error);

        // The digits of the scaled integer with the point put back, parsed as V1 parsed the text of the value.
        var digits = BigInteger.Abs(new DuckDBHugeInt(scaled.lower, scaled.upper).ToBigInteger()).ToString(CultureInfo.InvariantCulture).PadLeft(scale + 1, '0');
        var text = scale == 0 ? digits : digits[..^scale] + "." + digits[^scale..];
        var number = decimal.Parse(text, NumberStyles.Any, CultureInfo.InvariantCulture);
        return scaled.upper < 0 ? -number : number;
    }

    private static string GetVarchar(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        sbyte* text;
        ulong length;
        NativeMethodsV2.duckdb_v2_value_get_varchar(pointer, &text, &length, &error).ThrowIfFailed(ref error);
        return DuckDBV2Utf8.ToManaged(text, length);
    }

    private static DuckDBDateOnly GetDate(_duckdb_v2_value* pointer) =>
        DuckDBDateOnly.FromDuckDBDate(new DuckDBDate { Days = Get<int>(pointer, &NativeMethodsV2.duckdb_v2_value_get_date) });

    private static DuckDBTimeOnly GetTime(_duckdb_v2_value* pointer) =>
        NativeMethods.DateTimeHelpers.DuckDBFromTime(new DuckDBTime { Micros = Get<long>(pointer, &NativeMethodsV2.duckdb_v2_value_get_time) });

    private static DuckDBTimeNs GetTimeNs(_duckdb_v2_value* pointer) =>
        new() { Nanos = Get<long>(pointer, &NativeMethodsV2.duckdb_v2_value_get_time_ns) };

    // On 0001-01-02, like a TIMETZ read from a column: on the first day a DateTime holds, a time earlier than its own
    // positive offset would be an instant before year 1.
    private static DateTimeOffset GetTimeTz(_duckdb_v2_value* pointer)
    {
        var timeTz = NativeMethods.DateTimeHelpers.DuckDBFromTimeTz(new DuckDBTimeTzStruct { Bits = Get<ulong>(pointer, &NativeMethodsV2.duckdb_v2_value_get_time_tz) });
        return new DateTimeOffset(timeTz.Time.ToDateTime().AddDays(1), TimeSpan.FromSeconds(timeTz.Offset));
    }

    private static DuckDBInterval GetInterval(_duckdb_v2_value* pointer)
    {
        var interval = Get<duckdb_v2_interval_t>(pointer, &NativeMethodsV2.duckdb_v2_value_get_interval);
        return new DuckDBInterval(interval.months, interval.days, (ulong)interval.micros);
    }

    // The getters return each timestamp in its own unit; DuckDBTimestamp takes microseconds.
    private static DateTime GetTimestamp(long value, long microsPerUnit, int additionalTicks) =>
        DuckDBTimestamp.FromDuckDBTimestampStruct(new DuckDBTimestampStruct { Micros = value * microsPerUnit }).ToDateTime().AddTicks(additionalTicks);

    private static DateTime GetTimestampNs(long nanos) => GetTimestamp(nanos / 1000, 1, (int)(nanos % 1000 / 100));

    public void Dispose() => value.Dispose();
}
