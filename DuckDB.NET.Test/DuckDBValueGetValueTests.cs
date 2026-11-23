namespace DuckDB.NET.Test;

// DuckDBValue.GetValue<T> reads a parameter of a table function. It gives the value as the type it makes of the
// DuckDB type, or as object, a nullable or an interface of it, and refuses any other type. Before 2.0 it reinterpreted
// the value as T: a long read 8 bytes of a 4-byte INTEGER, and a string took the integer for a reference.
public class DuckDBValueGetValueTests
{
    [Fact]
    [Trait("V2", "Ready")]
    public void AValueReadsAsItsOwnType()
    {
        using var integer = NativeMethods.Value.DuckDBCreateInt32(42);
        integer.GetValue<int>().Should().Be(42);

        using var text = NativeMethods.Value.DuckDBCreateVarchar("duck");
        text.GetValue<string>().Should().Be("duck");

        using var huge = NativeMethods.Value.DuckDBCreateHugeInt(new DuckDBHugeInt((Int128)42));
        huge.GetValue<Int128>().Should().Be(42);
        huge.GetValue<BigInteger>().Should().Be(new BigInteger(42));
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void AValueReadsAsObjectNullableAndInterface()
    {
        using var integer = NativeMethods.Value.DuckDBCreateInt32(42);
        integer.GetValue<object>().Should().Be(42);
        integer.GetValue<int?>().Should().Be(42);
        integer.GetValue<IComparable<int>>().CompareTo(42).Should().Be(0);

        using var text = NativeMethods.Value.DuckDBCreateVarchar("duck");
        text.GetValue<object>().Should().Be("duck");
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void AValueIsRefusedAsAnotherType()
    {
        using var integer = NativeMethods.Value.DuckDBCreateInt32(42);
        integer.Invoking(v => v.GetValue<long>()).Should().Throw<InvalidCastException>().WithMessage("Cannot read a value of type Int32 as Int64.");
        integer.Invoking(v => v.GetValue<short>()).Should().Throw<InvalidCastException>();
        integer.Invoking(v => v.GetValue<string>()).Should().Throw<InvalidCastException>().WithMessage("Cannot read a value of type Int32 as String.");

        using var text = NativeMethods.Value.DuckDBCreateVarchar("duck");
        text.Invoking(v => v.GetValue<int>()).Should().Throw<InvalidCastException>();
    }

    // Every timestamp type reads as a DateTime, and as the instant at UTC when it is read as DateTimeOffset, as the
    // column reader gives it. Each getter takes the value in the unit of its type.
    [Fact]
    [Trait("V2", "Ready")]
    public void ATimestampReadsAsDateTimeAndAsDateTimeOffsetAtUtc()
    {
        var expected = new DateTime(2024, 2, 29, 13, 45, 30);
        var micros = (expected - DateTime.UnixEpoch).Ticks / 10;

        using var timestamp = NativeMethods.Value.DuckDBCreateTimestamp(new DuckDBTimestampStruct { Micros = micros + 123456 });
        using var timestampTz = NativeMethods.Value.DuckDBCreateTimestampTz(new DuckDBTimestampStruct { Micros = micros + 123456 });
        using var timestampS = NativeMethods.Value.DuckDBCreateTimestampS(new DuckDBTimestampStruct { Micros = micros / 1_000_000 });
        using var timestampMs = NativeMethods.Value.DuckDBCreateTimestampMs(new DuckDBTimestampStruct { Micros = micros / 1000 + 123 });
        using var timestampNs = NativeMethods.Value.DuckDBCreateTimestampNs(new DuckDBTimestampStruct { Micros = micros * 1000 + 123_456_700 });

        var cases = new (DuckDBValue Value, DateTime Expected)[]
        {
            (timestamp, expected.AddTicks(1_234_560)),
            (timestampTz, expected.AddTicks(1_234_560)),
            (timestampS, expected),
            (timestampMs, expected.AddTicks(1_230_000)),
            (timestampNs, expected.AddTicks(1_234_567)),
        };

        foreach (var (value, dateTime) in cases)
        {
            value.GetValue<DateTime>().Should().Be(dateTime);

            var dateTimeOffset = value.GetValue<DateTimeOffset>();
            dateTimeOffset.Offset.Should().Be(TimeSpan.Zero);
            dateTimeOffset.UtcTicks.Should().Be(dateTime.Ticks);

            value.GetValue<DateTimeOffset?>().Should().Be(dateTimeOffset);
            value.GetValue<object>().Should().Be(dateTime);
        }
    }
}
