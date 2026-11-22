namespace DuckDB.NET.Native;

public class DuckDBValue() : SafeHandleZeroOrMinusOneIsInvalid(true), IDuckDBValueReader
{
    private DuckDBValue[] childValues = [];

    protected override bool ReleaseHandle()
    {
        foreach (var value in childValues)
        {
            value.Dispose();
        }

        NativeMethods.Value.DuckDBDestroyValue(ref handle);
        return true;
    }

    internal void SetChildValues(DuckDBValue[] values)
    {
        childValues = values;
    }

    public bool IsNull() => NativeMethods.Value.DuckDBIsNullValue(this);

    public T GetValue<T>()
    {
        var logicalType = NativeMethods.Value.DuckDBGetValueType(this);

        //Logical type is part of the duckdb_value object and it shouldn't be released separately
        //It will get released when the duckdb_value object is destroyed below.
        var add = false;
        logicalType.DangerousAddRef(ref add);

        var duckDBType = NativeMethods.LogicalType.DuckDBGetTypeId(logicalType);

        return duckDBType switch
        {
            DuckDBType.Boolean => Cast(NativeMethods.Value.DuckDBGetBool(this)),

            DuckDBType.TinyInt => Cast(NativeMethods.Value.DuckDBGetInt8(this)),
            DuckDBType.SmallInt => Cast(NativeMethods.Value.DuckDBGetInt16(this)),
            DuckDBType.Integer => Cast(NativeMethods.Value.DuckDBGetInt32(this)),
            DuckDBType.BigInt => Cast(NativeMethods.Value.DuckDBGetInt64(this)),

            DuckDBType.UnsignedTinyInt => Cast(NativeMethods.Value.DuckDBGetUInt8(this)),
            DuckDBType.UnsignedSmallInt => Cast(NativeMethods.Value.DuckDBGetUInt16(this)),
            DuckDBType.UnsignedInteger => Cast(NativeMethods.Value.DuckDBGetUInt32(this)),
            DuckDBType.UnsignedBigInt => Cast(NativeMethods.Value.DuckDBGetUInt64(this)),

            DuckDBType.Float => Cast(NativeMethods.Value.DuckDBGetFloat(this)),
            DuckDBType.Double => Cast(NativeMethods.Value.DuckDBGetDouble(this)),

            DuckDBType.Decimal => Cast(decimal.Parse(NativeMethods.Value.DuckDBGetVarchar(this), NumberStyles.Any, CultureInfo.InvariantCulture)),

            DuckDBType.Uuid => Cast(new Guid(NativeMethods.Value.DuckDBGetVarchar(this))),

            // An Int128 or a UInt128 is produced as itself; any other type gets the BigInteger.
            DuckDBType.HugeInt when typeof(T) == typeof(Int128) => Cast(NativeMethods.Value.DuckDBGetHugeInt(this).ToInt128()),
            DuckDBType.HugeInt => Cast(NativeMethods.Value.DuckDBGetHugeInt(this).ToBigInteger()),
            DuckDBType.UnsignedHugeInt when typeof(T) == typeof(UInt128) => Cast(NativeMethods.Value.DuckDBGetUHugeInt(this).ToUInt128()),
            DuckDBType.UnsignedHugeInt => Cast(NativeMethods.Value.DuckDBGetUHugeInt(this).ToBigInteger()),

            DuckDBType.Varchar => Cast(NativeMethods.Value.DuckDBGetVarchar(this)),

            // DATE and TIME can be read as the .NET types or as the provider's own types.
            DuckDBType.Date when typeof(T) == typeof(DuckDBDateOnly) => Cast(DuckDBDateOnly.FromDuckDBDate(NativeMethods.Value.DuckDBGetDate(this))),
            DuckDBType.Date => Cast((DateOnly)DuckDBDateOnly.FromDuckDBDate(NativeMethods.Value.DuckDBGetDate(this))),
            DuckDBType.Time when typeof(T) == typeof(DuckDBTimeOnly) => Cast(NativeMethods.DateTimeHelpers.DuckDBFromTime(NativeMethods.Value.DuckDBGetTime(this))),
            DuckDBType.Time => Cast((TimeOnly)NativeMethods.DateTimeHelpers.DuckDBFromTime(NativeMethods.Value.DuckDBGetTime(this))),
            DuckDBType.TimeNs when typeof(T) == typeof(DuckDBTimeOnly) => Cast(NativeMethods.Value.DuckDBGetTimeNs(this).ToDuckDBTimeOnly()),
            DuckDBType.TimeNs => Cast(new TimeOnly(NativeMethods.Value.DuckDBGetTimeNs(this).Ticks)),
            DuckDBType.TimeTz => Cast(GetTimeTzValue()),
            DuckDBType.Interval => Cast((TimeSpan)NativeMethods.Value.DuckDBGetInterval(this)),
            DuckDBType.Timestamp => CastTimestamp(GetTimestampValue(NativeMethods.Value.DuckDBGetTimestamp(this), DuckDBType.Timestamp)),
            DuckDBType.TimestampS => CastTimestamp(GetTimestampValue(NativeMethods.Value.DuckDBGetTimestampS(this), DuckDBType.TimestampS)),
            DuckDBType.TimestampMs => CastTimestamp(GetTimestampValue(NativeMethods.Value.DuckDBGetTimestampMs(this), DuckDBType.TimestampMs)),
            DuckDBType.TimestampNs => CastTimestamp(GetTimestampValue(NativeMethods.Value.DuckDBGetTimestampNs(this), DuckDBType.TimestampNs)),
            DuckDBType.TimestampTz => CastTimestamp(GetTimestampValue(NativeMethods.Value.DuckDBGetTimestamp(this), DuckDBType.TimestampTz)),
            _ => throw new NotImplementedException($"Cannot read value of type {typeof(T).FullName}")
        };

        // The value as it was produced, or as object, a nullable or an interface of it. Any other type is refused: the
        // value used to be reinterpreted as T, which read past it or took an integer for a reference.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static T Cast<TSource>(TSource value) => typeof(T) == typeof(TSource)
            ? Unsafe.As<TSource, T>(ref value)
            : value is T result ? result : throw CannotCast(typeof(TSource), typeof(T));

        // A timestamp is a DateTime, or the instant at UTC when it is read as DateTimeOffset, as the column reader
        // gives it. GetTimestampValue builds a DateTime of kind Unspecified, which an offset of zero accepts.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static T CastTimestamp(DateTime value) => typeof(T) == typeof(DateTimeOffset) || typeof(T) == typeof(DateTimeOffset?)
            ? Cast(new DateTimeOffset(value, TimeSpan.Zero))
            : Cast(value);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static InvalidCastException CannotCast(Type from, Type to) => new($"Cannot read a value of type {from.Name} as {to.Name}.");

    private DateTime GetTimestampValue(DuckDBTimestampStruct timestampStruct, DuckDBType duckDBType)
    {
        var additionalTicks = 0;

        // The type-specific getters return values in their native units:
        // - TimestampS: seconds
        // - TimestampMs: milliseconds
        // - TimestampNs: nanoseconds
        // We need to convert to microseconds for DuckDBTimestamp.FromDuckDBTimestampStruct()
        if (duckDBType == DuckDBType.TimestampNs)
        {
            additionalTicks = (int)(timestampStruct.Micros % 1000 / 100);
            timestampStruct.Micros /= 1000;
        }
        if (duckDBType == DuckDBType.TimestampMs)
        {
            timestampStruct.Micros *= 1000;
        }
        if (duckDBType == DuckDBType.TimestampS)
        {
            timestampStruct.Micros *= 1000000;
        }

        var timestamp = DuckDBTimestamp.FromDuckDBTimestampStruct(timestampStruct);
        return timestamp.ToDateTime().AddTicks(additionalTicks);
    }

    private DateTimeOffset GetTimeTzValue()
    {
        var timeTzStruct = NativeMethods.Value.DuckDBGetTimeTz(this);
        var timeTz = NativeMethods.DateTimeHelpers.DuckDBFromTimeTz(timeTzStruct);

        // On 0001-01-02, like a TIMETZ read from a column: on the first day a DateTime holds, a time earlier than its
        // own positive offset would be an instant before year 1.
        return new DateTimeOffset(timeTz.Time.ToDateTime().AddDays(1), TimeSpan.FromSeconds(timeTz.Offset));
    }
}
