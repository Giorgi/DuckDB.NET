namespace DuckDB.NET.Data.PreparedStatement;

// Converts the value of a parameter to the CLR type of the DbType the caller assigned, before ClrToDuckDBConverter
// fits it to the parameter's type. Within a family a value is converted only when nothing is lost: 2.0 with
// DbType.Int32 is 2, 2.5 is refused, and so is a value of another family ("42" with DbType.Int32, 42 with
// DbType.String). The two deliberate exceptions: a DateTime with DbType.Date or DbType.Time keeps only its date or its
// time, and a double or float with DbType.Decimal goes through the C# cast (0.1 is 0.1m), as the reader converts a
// DOUBLE read as decimal. DbType.Object converts nothing. A refused value throws an InvalidCastException that names
// the parameter.
internal static class DbTypeConverter
{
    // duckDBType is the parameter's type as DuckDB reports it (Invalid when it reports none): two pairs depend on it.
    public static object Convert(object item, DbType dbType, DuckDBType duckDBType, ParameterLabel parameter)
    {
        var target = new Target(dbType, duckDBType, parameter);

        return dbType switch
        {
            DbType.SByte => ToNumber<sbyte>(item, target),
            DbType.Int16 => ToNumber<short>(item, target),
            DbType.Int32 => ToNumber<int>(item, target),
            DbType.Int64 => ToNumber<long>(item, target),
            DbType.Byte => ToNumber<byte>(item, target),
            DbType.UInt16 => ToNumber<ushort>(item, target),
            DbType.UInt32 => ToNumber<uint>(item, target),
            DbType.UInt64 => ToNumber<ulong>(item, target),
            // An Int128 and a UInt128 keep their own type, which has a creator: a UInt128 stays a UHUGEINT.
            DbType.VarNumeric => item is Int128 or UInt128 ? item : ToNumber<BigInteger>(item, target),
            DbType.Single => ToNumber<float>(item, target),
            DbType.Double => ToNumber<double>(item, target),
            DbType.Decimal or DbType.Currency => ToNumber<decimal>(item, target),
            DbType.String or DbType.AnsiString or DbType.StringFixedLength or DbType.AnsiStringFixedLength or DbType.Xml => ToText(item, target),
            DbType.Boolean => item is bool ? item : throw Refuse(item, target, typeof(bool)),
            DbType.Guid => item is Guid ? item : throw Refuse(item, target, typeof(Guid)),
            DbType.Binary => item is byte[] ? item : throw Refuse(item, target, typeof(byte[])),
            DbType.Date => ToDate(item, target),
            DbType.Time => ToTime(item, target),
            DbType.DateTime or DbType.DateTime2 => ToTimestamp(item, target),
            DbType.DateTimeOffset => ToTimestampWithTimeZone(item, target),
            _ => item
        };
    }

    // Any number, as T. An integer target takes an enum as its number, and a float, double or decimal only when it is
    // whole; a floating-point target takes any number by the C# cast; a decimal target takes a value that fits, a float
    // or double by the C# cast (0.1 is 0.1m). Every target but a floating-point one refuses a value out of its range.
    // shown is the value the message names: an enum, not the number it is converted as.
    private static object ToNumber<T>(object item, Target target, object? shown = null) where T : struct, INumberBase<T>
    {
        shown ??= item;

        return item switch
        {
            T => item,
            sbyte value => Convert<sbyte, T>(value, shown, target),
            byte value => Convert<byte, T>(value, shown, target),
            short value => Convert<short, T>(value, shown, target),
            ushort value => Convert<ushort, T>(value, shown, target),
            int value => Convert<int, T>(value, shown, target),
            uint value => Convert<uint, T>(value, shown, target),
            long value => Convert<long, T>(value, shown, target),
            ulong value => Convert<ulong, T>(value, shown, target),
            Int128 value => Convert<Int128, T>(value, shown, target),
            UInt128 value => Convert<UInt128, T>(value, shown, target),
            BigInteger value => Convert<BigInteger, T>(value, shown, target),
            nint value => Convert<nint, T>(value, shown, target),
            nuint value => Convert<nuint, T>(value, shown, target),
            Half value => Convert<Half, T>(value, shown, target),
            float value => Convert<float, T>(value, shown, target),
            double value => Convert<double, T>(value, shown, target),
            decimal value => Convert<decimal, T>(value, shown, target),
            Enum when IsInteger<T>() => ToNumber<T>(ClrToDuckDBConverter.EnumNumber(item), target, item),
            _ => throw Refuse(item, target, typeof(T))
        };
    }

    private static T Convert<TFrom, T>(TFrom value, object item, Target target) where TFrom : INumberBase<TFrom> where T : INumberBase<T>
    {
        if (typeof(T) == typeof(float) || typeof(T) == typeof(double))
        {
            return T.CreateTruncating(value);
        }

        if (IsInteger<T>() && !TFrom.IsInteger(value))
        {
            throw Refuse(item, target, typeof(T), TFrom.IsFinite(value) ? Reason.Fraction : Reason.OutOfRange);
        }

        // The C# cast with an overflow check: an integer narrowed, a whole double to BigInteger exactly, a float or
        // double to decimal rounded to 15 significant digits (7 for a float). It throws OverflowException out of range
        // and for NaN.
        T result;
        try
        {
            result = T.CreateChecked(value);
        }
        catch (OverflowException)
        {
            throw Refuse(item, target, typeof(T), Reason.OutOfRange);
        }

        // Nor is a value too small for a decimal (1e-30) sent as zero.
        return T.IsZero(result) && !TFrom.IsZero(value) ? throw Refuse(item, target, typeof(T), Reason.OutOfRange) : result;
    }

    // Folded by the JIT: every numeric target but float, double and decimal holds only whole numbers.
    private static bool IsInteger<T>() => typeof(T) != typeof(float) && typeof(T) != typeof(double) && typeof(T) != typeof(decimal);

    private static object ToText(object item, Target target)
    {
        return item switch
        {
            string => item,
            char value => value.ToString(),
            _ => throw Refuse(item, target, typeof(string))
        };
    }

    // A DateTime keeps its date: the time is dropped, as in Npgsql.
    private static object ToDate(object item, Target target)
    {
        return item switch
        {
            DateOnly or DuckDBDateOnly => item,
            DateTime value => DateOnly.FromDateTime(value),
            _ => throw Refuse(item, target, typeof(DateOnly))
        };
    }

    // A TimeSpan is a time of day, so it must be at least zero and less than a day; a DateTime keeps its time. For an
    // INTERVAL parameter a TimeSpan stays as it is: DuckDB has no cast from TIME to INTERVAL, and SqlClient's
    // convention gives every TimeSpan DbType.Time.
    private static object ToTime(object item, Target target)
    {
        return item switch
        {
            TimeOnly or DuckDBTimeOnly => item,
            TimeSpan when target.DuckDBType == DuckDBType.Interval => item,
            TimeSpan value when value >= TimeSpan.Zero && value < TimeSpan.FromDays(1) => TimeOnly.FromTimeSpan(value),
            TimeSpan => throw Refuse(item, target, typeof(TimeOnly), Reason.OutOfRange),
            DateTime value => TimeOnly.FromDateTime(value),
            _ => throw Refuse(item, target, typeof(TimeOnly))
        };
    }

    // A DateOnly is its midnight.
    private static object ToTimestamp(object item, Target target)
    {
        return item switch
        {
            DateTime or DuckDBTimestamp => item,
            DateOnly value => value.ToDateTime(TimeOnly.MinValue),
            _ => throw Refuse(item, target, typeof(DateTime))
        };
    }

    // A DateTime becomes a DateTimeOffset only where DuckDB reports no type: its date and time are taken as UTC,
    // whatever its Kind, as a TIMESTAMPTZ parameter takes them. Where it reports one, the DateTime stays as it is, so a
    // TIMESTAMP keeps the clock time instead of having it moved by the session's time zone.
    private static object ToTimestampWithTimeZone(object item, Target target)
    {
        return item switch
        {
            DateTimeOffset => item,
            DateTime when target.DuckDBType != DuckDBType.Invalid => item,
            DateTime value => new DateTimeOffset(value.Ticks, TimeSpan.Zero),
            _ => throw Refuse(item, target, typeof(DateTimeOffset))
        };
    }

    // The DbType, the parameter's type and the parameter, for the rules that depend on them and for the message of a
    // refused value.
    private readonly record struct Target(DbType DbType, DuckDBType DuckDBType, ParameterLabel Parameter);

    private enum Reason
    {
        Type,
        Fraction,
        OutOfRange
    }

    private static InvalidCastException Refuse(object item, Target target, Type type, Reason reason = Reason.Type)
    {
        var detail = reason switch
        {
            Reason.Fraction => ": the value has a non-zero fractional part",
            Reason.OutOfRange => ": the value is out of range",
            _ when item is string => ". Remove the DbType to let DuckDB convert the text, or parse the value first",
            _ => ""
        };

        return new InvalidCastException($"Cannot cast from {Name(item.GetType())} to {type.Name} for parameter {target.Parameter} (DbType.{target.DbType}){detail}");
    }

    // A generic type as C# writes it, HashSet<Int64> rather than HashSet`1.
    private static string Name(Type type)
    {
        if (!type.IsGenericType)
        {
            return type.Name;
        }

        var name = type.Name;
        return $"{name[..name.IndexOf('`')]}<{string.Join(", ", Array.ConvertAll(type.GetGenericArguments(), Name))}>";
    }
}

// The parameter a value belongs to: its name when it has one, otherwise its position ($1). Formatted only for the
// message of a refused value.
internal readonly record struct ParameterLabel(long Index, string? Name)
{
    public override string ToString() => string.IsNullOrEmpty(Name) ? $"${Index}" : $"\"{Name}\"";
}
