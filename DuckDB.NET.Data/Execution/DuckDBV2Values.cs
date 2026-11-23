using System.Text;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Execution;

/// <summary>
/// Creates the V2 values bound to parameters. Every value is created through the connection's factory, which reaches
/// its catalog and session settings. The factory is passed as an <see cref="IntPtr"/> so the converter can keep it in
/// delegates; it is borrowed from the connection and valid while the connection is open.
/// </summary>
internal static unsafe class DuckDBV2Values
{
    private static DuckDBV2Value Created(DUCKDB_V2_ERROR code, _duckdb_v2_value* value, ref _duckdb_v2_error_info* error)
    {
        code.ThrowIfFailed(ref error, "Creating the value of a parameter failed");
        return new DuckDBV2Value(value);
    }

    private static _duckdb_v2_factory* F(IntPtr factory) => (_duckdb_v2_factory*)factory;

    /// <summary>
    /// A NULL of DuckDB's NULL type, as V1's untyped NULL was: DuckDB casts it to the parameter's type. A NULL of the
    /// UNKNOWN type that the parameter schema reports for an untyped parameter counts as no value at all ("Not all
    /// parameters were bound").
    /// </summary>
    public static DuckDBV2Value Null(IntPtr factory)
    {
        using var type = DuckDBV2Types.FromText(factory, "NULL");
        using var lease = type.Acquire();

        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_null(F(factory), lease.Pointer, &value, &error);
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value Bool(IntPtr factory, bool input)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_bool(F(factory), input ? (byte)1 : (byte)0, &value, &error);
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value Int8(IntPtr factory, sbyte input)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_tinyint(F(factory), input, &value, &error);
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value Int16(IntPtr factory, short input)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_smallint(F(factory), input, &value, &error);
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value Int32(IntPtr factory, int input)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_int(F(factory), input, &value, &error);
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value Int64(IntPtr factory, long input)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_bigint(F(factory), input, &value, &error);
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value UInt8(IntPtr factory, byte input)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_utinyint(F(factory), input, &value, &error);
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value UInt16(IntPtr factory, ushort input)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_usmallint(F(factory), input, &value, &error);
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value UInt32(IntPtr factory, uint input)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_uint(F(factory), input, &value, &error);
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value UInt64(IntPtr factory, ulong input)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_ubigint(F(factory), input, &value, &error);
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value HugeInt(IntPtr factory, Int128 input)
    {
        var hugeint = new duckdb_v2_hugeint_t { lower = (ulong)input, upper = (long)(input >> 64) };

        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_hugeint(F(factory), &hugeint, &value, &error);
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value UHugeInt(IntPtr factory, UInt128 input)
    {
        var uhugeint = new duckdb_v2_uhugeint_t { lower = (ulong)input, upper = (ulong)(input >> 64) };

        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_uhugeint(F(factory), &uhugeint, &value, &error);
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value Float(IntPtr factory, float input)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_float(F(factory), input, &value, &error);
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value Double(IntPtr factory, double input)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_double(F(factory), input, &value, &error);
        return Created(code, value, ref error);
    }

    /// <summary>The integer and scale of a DECIMAL of the given width.</summary>
    public static DuckDBV2Value Decimal(IntPtr factory, Int128 integer, byte width, byte scale)
    {
        var hugeint = new duckdb_v2_hugeint_t { lower = (ulong)integer, upper = (long)(integer >> 64) };

        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_decimal(F(factory), &hugeint, width, scale, &value, &error);
        return Created(code, value, ref error);
    }

    /// <summary>A UUID in DuckDB's storage form, with the top bit flipped (<see cref="GuidConverter.ToHugeInt"/>).</summary>
    public static DuckDBV2Value Uuid(IntPtr factory, DuckDBHugeInt storage)
    {
        var hugeint = new duckdb_v2_hugeint_t { lower = storage.Lower, upper = storage.Upper };

        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_uuid(F(factory), &hugeint, &value, &error);
        return Created(code, value, ref error);
    }

    /// <summary>Text as UTF-8 with its length, so a NUL character in it is kept.</summary>
    public static DuckDBV2Value Varchar(IntPtr factory, string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);

        fixed (byte* pointer = bytes)
        {
            _duckdb_v2_error_info* error = null;
            _duckdb_v2_value* value;
            var code = NativeMethodsV2.duckdb_v2_value_create_varchar(F(factory), (sbyte*)pointer, (ulong)bytes.Length, &value, &error);
            return Created(code, value, ref error);
        }
    }

    public static DuckDBV2Value Blob(IntPtr factory, byte[] bytes)
    {
        fixed (byte* pointer = bytes)
        {
            _duckdb_v2_error_info* error = null;
            _duckdb_v2_value* value;
            var code = NativeMethodsV2.duckdb_v2_value_create_blob(F(factory), (sbyte*)pointer, (ulong)bytes.Length, &value, &error);
            return Created(code, value, ref error);
        }
    }

    /// <summary>
    /// A BIGNUM of any size. V2 takes DuckDB's storage bytes, which <c>bignum_encode</c> makes from the big-endian
    /// magnitude and the sign; V1 took the magnitude and the sign directly.
    /// </summary>
    public static DuckDBV2Value Bignum(IntPtr factory, BigInteger input)
    {
        var isNegative = input.Sign < 0;
        var magnitude = (isNegative ? -input : input).ToByteArray(isUnsigned: true, isBigEndian: true);

        fixed (byte* magnitudePointer = magnitude)
        {
            _duckdb_v2_error_info* error = null;
            ulong length;
            NativeMethodsV2.duckdb_v2_bignum_encode(magnitudePointer, (ulong)magnitude.Length, isNegative ? (byte)1 : (byte)0, null, 0, &length, &error)
                           .ThrowIfFailed(ref error, "Encoding a BIGNUM failed");

            var storage = new byte[length];

            fixed (byte* storagePointer = storage)
            {
                NativeMethodsV2.duckdb_v2_bignum_encode(magnitudePointer, (ulong)magnitude.Length, isNegative ? (byte)1 : (byte)0, storagePointer, length, &length, &error)
                               .ThrowIfFailed(ref error, "Encoding a BIGNUM failed");

                _duckdb_v2_value* value;
                var code = NativeMethodsV2.duckdb_v2_value_create_bignum(F(factory), (sbyte*)storagePointer, length, &value, &error);
                return Created(code, value, ref error);
            }
        }
    }

    public static DuckDBV2Value Date(IntPtr factory, DuckDBDate date)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_date(F(factory), date.Days, &value, &error);
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value Time(IntPtr factory, DuckDBTime time)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_time(F(factory), time.Micros, &value, &error);
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value TimeNs(IntPtr factory, DuckDBTimeNs time)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_time_ns(F(factory), time.Nanos, &value, &error);
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value TimeTz(IntPtr factory, DuckDBTimeTzStruct time)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_time_tz(F(factory), time.Bits, &value, &error);
        return Created(code, value, ref error);
    }

    /// <summary>A TIMESTAMP of any precision: the struct holds the count in the unit of <paramref name="type"/>.</summary>
    public static DuckDBV2Value Timestamp(IntPtr factory, DuckDBTimestampStruct timestamp, DuckDBType type)
    {
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = type switch
        {
            DuckDBType.TimestampS => NativeMethodsV2.duckdb_v2_value_create_timestamp_sec(F(factory), timestamp.Micros, &value, &error),
            DuckDBType.TimestampMs => NativeMethodsV2.duckdb_v2_value_create_timestamp_ms(F(factory), timestamp.Micros, &value, &error),
            DuckDBType.TimestampNs => NativeMethodsV2.duckdb_v2_value_create_timestamp_ns(F(factory), timestamp.Micros, &value, &error),
            DuckDBType.TimestampTz => NativeMethodsV2.duckdb_v2_value_create_timestamp_tz(F(factory), timestamp.Micros, &value, &error),
            _ => NativeMethodsV2.duckdb_v2_value_create_timestamp(F(factory), timestamp.Micros, &value, &error),
        };
        return Created(code, value, ref error);
    }

    public static DuckDBV2Value Interval(IntPtr factory, DuckDBInterval interval)
    {
        var input = new duckdb_v2_interval_t { months = interval.Months, days = interval.Days, micros = (long)interval.Micros };

        _duckdb_v2_error_info* error = null;
        _duckdb_v2_value* value;
        var code = NativeMethodsV2.duckdb_v2_value_create_interval(F(factory), &input, &value, &error);
        return Created(code, value, ref error);
    }

    /// <summary>
    /// A LIST or ARRAY of the given item type. The items are copied in: the caller still owns them, and disposes them
    /// whether or not this succeeds.
    /// </summary>
    public static DuckDBV2Value Collection(IntPtr factory, IntPtr itemType, DuckDBV2Value[] items, bool isList)
    {
        var pointers = new IntPtr[items.Length];
        var leases = 0;

        try
        {
            for (; leases < items.Length; leases++)
            {
                var added = false;
                items[leases].DangerousAddRef(ref added);
                pointers[leases] = items[leases].DangerousGetHandle();
            }

            fixed (IntPtr* children = pointers)
            {
                _duckdb_v2_error_info* error = null;
                _duckdb_v2_value* value;
                var code = isList
                    ? NativeMethodsV2.duckdb_v2_value_create_list(F(factory), (_duckdb_v2_logical_type*)itemType, (_duckdb_v2_value**)children, (ulong)items.Length, &value, &error)
                    : NativeMethodsV2.duckdb_v2_value_create_array(F(factory), (_duckdb_v2_logical_type*)itemType, (_duckdb_v2_value**)children, (ulong)items.Length, &value, &error);
                return Created(code, value, ref error);
            }
        }
        finally
        {
            for (var index = 0; index < leases; index++)
            {
                items[index].DangerousRelease();
            }
        }
    }
}
