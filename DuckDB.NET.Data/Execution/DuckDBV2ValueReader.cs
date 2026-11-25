using System.Globalization;
using System.Numerics;
using System.Runtime.CompilerServices;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Execution;

/// <summary>
/// A parameter of a table function as the function's bind callback reads it: a V2 value, read with the conversions
/// that V1's <c>DuckDBValue.GetValue&lt;T&gt;</c> had, case for case. A value is produced as its own .NET type and refused as
/// any other, except <c>object</c>, a nullable or an interface of it; HUGEINT and UHUGEINT are <see cref="Int128"/> and
/// <see cref="UInt128"/>, and also <see cref="BigInteger"/>; a timestamp is also a <see cref="DateTimeOffset"/> at UTC; DATE and
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
            DUCKDB_V2_LOGICAL_TYPE_ID.BOOLEAN => Cast<T, bool>(ValueGetBool(pointer)),

            DUCKDB_V2_LOGICAL_TYPE_ID.TINYINT => Cast<T, sbyte>(ValueGetTinyint(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.SMALLINT => Cast<T, short>(ValueGetSmallint(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.INTEGER => Cast<T, int>(ValueGetInt(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.BIGINT => Cast<T, long>(ValueGetBigint(pointer)),

            DUCKDB_V2_LOGICAL_TYPE_ID.UTINYINT => Cast<T, byte>(ValueGetUtinyint(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.USMALLINT => Cast<T, ushort>(ValueGetUsmallint(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.UINTEGER => Cast<T, uint>(ValueGetUint(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.UBIGINT => Cast<T, ulong>(ValueGetUbigint(pointer)),

            DUCKDB_V2_LOGICAL_TYPE_ID.FLOAT => Cast<T, float>(ValueGetFloat(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.DOUBLE => Cast<T, double>(ValueGetDouble(pointer)),

            // V1 parsed the text of the value; this is the same number, from its scaled integer.
            DUCKDB_V2_LOGICAL_TYPE_ID.DECIMAL => Cast<T, decimal>(GetDecimal(pointer)),

            DUCKDB_V2_LOGICAL_TYPE_ID.UUID => Cast<T, Guid>(ToHugeInt(ValueGetUuid(pointer)).ConvertToGuid()),

            // HUGEINT and UHUGEINT are produced as Int128 and UInt128, as the column readers give them, or as a
            // BigInteger when one is asked for.
            DUCKDB_V2_LOGICAL_TYPE_ID.HUGEINT when Is<T, BigInteger>() => Cast<T, BigInteger>(ToHugeInt(ValueGetHugeint(pointer)).ToBigInteger()),
            DUCKDB_V2_LOGICAL_TYPE_ID.HUGEINT => Cast<T, Int128>(ToHugeInt(ValueGetHugeint(pointer)).ToInt128()),
            DUCKDB_V2_LOGICAL_TYPE_ID.UHUGEINT when Is<T, BigInteger>() => Cast<T, BigInteger>(GetUHugeInt(pointer).ToBigInteger()),
            DUCKDB_V2_LOGICAL_TYPE_ID.UHUGEINT => Cast<T, UInt128>(GetUHugeInt(pointer).ToUInt128()),

            DUCKDB_V2_LOGICAL_TYPE_ID.VARCHAR => Cast<T, string>(GetVarchar(pointer)),

            // DATE and TIME can be read as the .NET types or as the provider's own types.
            DUCKDB_V2_LOGICAL_TYPE_ID.DATE when Is<T, DuckDBDateOnly>() => Cast<T, DuckDBDateOnly>(GetDate(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.DATE => Cast<T, DateOnly>((DateOnly)GetDate(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIME when Is<T, DuckDBTimeOnly>() => Cast<T, DuckDBTimeOnly>(GetTime(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIME => Cast<T, TimeOnly>((TimeOnly)GetTime(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIME_NS when Is<T, DuckDBTimeOnly>() => Cast<T, DuckDBTimeOnly>(GetTimeNs(pointer).ToDuckDBTimeOnly()),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIME_NS => Cast<T, TimeOnly>(new TimeOnly(GetTimeNs(pointer).Ticks)),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIME_TZ => Cast<T, DateTimeOffset>(GetTimeTz(pointer)),
            DUCKDB_V2_LOGICAL_TYPE_ID.INTERVAL => Cast<T, TimeSpan>((TimeSpan)GetInterval(pointer)),

            DUCKDB_V2_LOGICAL_TYPE_ID.TIMESTAMP => CastTimestamp<T>(GetTimestamp(ValueGetTimestamp(pointer), 1, 0)),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIMESTAMP_SEC => CastTimestamp<T>(GetTimestamp(ValueGetTimestampSec(pointer), 1_000_000, 0)),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIMESTAMP_MS => CastTimestamp<T>(GetTimestamp(ValueGetTimestampMs(pointer), 1_000, 0)),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIMESTAMP_NS => CastTimestamp<T>(GetTimestampNs(ValueGetTimestampNs(pointer))),
            DUCKDB_V2_LOGICAL_TYPE_ID.TIMESTAMP_TZ => CastTimestamp<T>(GetTimestamp(ValueGetTimestampTz(pointer), 1, 0)),

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
    private static T CastTimestamp<T>(DateTime timestamp) => Is<T, DateTimeOffset>()
        ? Cast<T, DateTimeOffset>(new DateTimeOffset(timestamp, TimeSpan.Zero))
        : Cast<T, DateTime>(timestamp);

    // Whether T is TTarget or its nullable, so that an alternative type can be asked for in either form. The JIT folds
    // it to a constant for each T.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool Is<T, TTarget>() where TTarget : struct => typeof(T) == typeof(TTarget) || typeof(T) == typeof(TTarget?);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static InvalidCastException CannotCast(Type from, Type to) => new($"Cannot read a value of type {from.Name} as {to.Name}.");

    // One method per getter, each calling its P/Invoke directly. A shared helper taking the getter as a
    // delegate* managed pointer cost about 4 ns a call more: through the pointer, the call goes through the P/Invoke's
    // stub (docs/V2-MIGRATION.md, "SuppressGCTransition").

    private static long ValueGetBigint(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        long result;
        NativeMethodsV2.duckdb_v2_value_get_bigint(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static bool ValueGetBool(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        bool result;
        NativeMethodsV2.duckdb_v2_value_get_bool(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static int ValueGetDate(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        int result;
        NativeMethodsV2.duckdb_v2_value_get_date(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static double ValueGetDouble(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        double result;
        NativeMethodsV2.duckdb_v2_value_get_double(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static float ValueGetFloat(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        float result;
        NativeMethodsV2.duckdb_v2_value_get_float(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static duckdb_v2_hugeint_t ValueGetHugeint(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        duckdb_v2_hugeint_t result;
        NativeMethodsV2.duckdb_v2_value_get_hugeint(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static int ValueGetInt(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        int result;
        NativeMethodsV2.duckdb_v2_value_get_int(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static duckdb_v2_interval_t ValueGetInterval(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        duckdb_v2_interval_t result;
        NativeMethodsV2.duckdb_v2_value_get_interval(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static short ValueGetSmallint(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        short result;
        NativeMethodsV2.duckdb_v2_value_get_smallint(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static long ValueGetTime(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        long result;
        NativeMethodsV2.duckdb_v2_value_get_time(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static long ValueGetTimeNs(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        long result;
        NativeMethodsV2.duckdb_v2_value_get_time_ns(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static ulong ValueGetTimeTz(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        ulong result;
        NativeMethodsV2.duckdb_v2_value_get_time_tz(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static long ValueGetTimestamp(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        long result;
        NativeMethodsV2.duckdb_v2_value_get_timestamp(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static long ValueGetTimestampMs(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        long result;
        NativeMethodsV2.duckdb_v2_value_get_timestamp_ms(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static long ValueGetTimestampNs(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        long result;
        NativeMethodsV2.duckdb_v2_value_get_timestamp_ns(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static long ValueGetTimestampSec(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        long result;
        NativeMethodsV2.duckdb_v2_value_get_timestamp_sec(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static long ValueGetTimestampTz(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        long result;
        NativeMethodsV2.duckdb_v2_value_get_timestamp_tz(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static sbyte ValueGetTinyint(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        sbyte result;
        NativeMethodsV2.duckdb_v2_value_get_tinyint(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static ulong ValueGetUbigint(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        ulong result;
        NativeMethodsV2.duckdb_v2_value_get_ubigint(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static duckdb_v2_uhugeint_t ValueGetUhugeint(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        duckdb_v2_uhugeint_t result;
        NativeMethodsV2.duckdb_v2_value_get_uhugeint(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static uint ValueGetUint(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        uint result;
        NativeMethodsV2.duckdb_v2_value_get_uint(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static ushort ValueGetUsmallint(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        ushort result;
        NativeMethodsV2.duckdb_v2_value_get_usmallint(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static byte ValueGetUtinyint(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        byte result;
        NativeMethodsV2.duckdb_v2_value_get_utinyint(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static duckdb_v2_hugeint_t ValueGetUuid(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        duckdb_v2_hugeint_t result;
        NativeMethodsV2.duckdb_v2_value_get_uuid(pointer, &result, &error).ThrowIfFailed(ref error);
        return result;
    }

    private static DuckDBHugeInt ToHugeInt(duckdb_v2_hugeint_t hugeInt) => new(hugeInt.lower, hugeInt.upper);

    private static DuckDBUHugeInt GetUHugeInt(_duckdb_v2_value* pointer)
    {
        var hugeInt = ValueGetUhugeint(pointer);
        return new DuckDBUHugeInt(hugeInt.lower, hugeInt.upper);
    }

    private static decimal GetDecimal(_duckdb_v2_value* pointer)
    {
        _duckdb_v2_error_info* error = null;
        duckdb_v2_hugeint_t scaled;
        uint width, scale32;
        NativeMethodsV2.duckdb_v2_value_get_decimal(pointer, &scaled, &width, &scale32, &error).ThrowIfFailed(ref error);
        var scale = (int)scale32;

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
        DuckDBDateOnly.FromDuckDBDate(new DuckDBDate { Days = ValueGetDate(pointer) });

    private static DuckDBTimeOnly GetTime(_duckdb_v2_value* pointer) =>
        DuckDBTimeConversions.FromTime(ValueGetTime(pointer));

    private static DuckDBTimeNs GetTimeNs(_duckdb_v2_value* pointer) =>
        new() { Nanos = ValueGetTimeNs(pointer) };

    // On 0001-01-02, like a TIMETZ read from a column: on the first day a DateTime holds, a time earlier than its own
    // positive offset would be an instant before year 1.
    private static DateTimeOffset GetTimeTz(_duckdb_v2_value* pointer)
    {
        var timeTz = DuckDBTimeConversions.FromTimeTz(ValueGetTimeTz(pointer));
        return new DateTimeOffset(timeTz.Time.ToDateTime().AddDays(1), TimeSpan.FromSeconds(timeTz.Offset));
    }

    private static DuckDBInterval GetInterval(_duckdb_v2_value* pointer)
    {
        var interval = ValueGetInterval(pointer);
        return new DuckDBInterval(interval.months, interval.days, (ulong)interval.micros);
    }

    // The getters return each timestamp in its own unit; DuckDBTimestamp takes microseconds.
    private static DateTime GetTimestamp(long value, long microsPerUnit, int additionalTicks) =>
        DuckDBTimestamp.FromDuckDBTimestampStruct(new DuckDBTimestampStruct { Micros = value * microsPerUnit }).ToDateTime().AddTicks(additionalTicks);

    private static DateTime GetTimestampNs(long nanos) => GetTimestamp(nanos / 1000, 1, (int)(nanos % 1000 / 100));

    public void Dispose() => value.Dispose();
}
