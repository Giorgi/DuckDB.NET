using System.Numerics;

namespace DuckDB.NET.Test.V2;

// A parameter of a table function is read by DuckDBV2ValueReader, with the conversions of V1's DuckDBValue.GetValue,
// which DuckDBValueGetValueTests pins. These tests pin the same ones through a table function.
public class V2ValueReaderTests
{
    [Fact]
    public void ADateOrTimeReadsAsTheProviderTypeAndItsNullable()
    {
        using var connection = Open();

        Read<DuckDBDateOnly>(connection, DuckDBType.Date, "DATE '2024-02-29'").Should().Be(new DuckDBDateOnly(2024, 2, 29));
        Read<DuckDBDateOnly?>(connection, DuckDBType.Date, "DATE '2024-02-29'").Should().Be(new DuckDBDateOnly(2024, 2, 29));
        Read<DateOnly?>(connection, DuckDBType.Date, "DATE '2024-02-29'").Should().Be(new DateOnly(2024, 2, 29));

        var time = new DuckDBTimeOnly(13, 45, 30, 123456);
        foreach (var (type, literal) in new[] { (DuckDBType.Time, "TIME '13:45:30.123456'"), (DuckDBType.TimeNs, "'13:45:30.123456'::TIME_NS") })
        {
            Read<DuckDBTimeOnly>(connection, type, literal).Should().Be(time);
            Read<DuckDBTimeOnly?>(connection, type, literal).Should().Be(time);
            Read<TimeOnly?>(connection, type, literal).Should().Be(new TimeOnly(13, 45, 30).Add(TimeSpan.FromTicks(1_234_560)));
        }
    }

    [Fact]
    public void AHugeIntReadsAsInt128AndAsBigInteger()
    {
        using var connection = Open();
        const string min = "-170141183460469231731687303715884105728";

        Read<object>(connection, DuckDBType.HugeInt, $"'{min}'::HUGEINT").Should().Be(Int128.MinValue);
        Read<Int128?>(connection, DuckDBType.HugeInt, $"'{min}'::HUGEINT").Should().Be(Int128.MinValue);
        Read<BigInteger>(connection, DuckDBType.HugeInt, $"'{min}'::HUGEINT").Should().Be(BigInteger.Parse(min));
        Read<BigInteger?>(connection, DuckDBType.HugeInt, $"'{min}'::HUGEINT").Should().Be(BigInteger.Parse(min));

        Read<object>(connection, DuckDBType.UnsignedHugeInt, "'340282366920938463463374607431768211455'::UHUGEINT").Should().Be(UInt128.MaxValue);
        Read<BigInteger>(connection, DuckDBType.UnsignedHugeInt, "'340282366920938463463374607431768211455'::UHUGEINT").Should().Be((BigInteger)UInt128.MaxValue);
    }

    [Fact]
    public void ATimestampReadsAsDateTimeOffsetAndItsNullable()
    {
        using var connection = Open();
        var expected = new DateTimeOffset(2024, 2, 29, 13, 45, 30, TimeSpan.Zero);

        Read<DateTimeOffset>(connection, DuckDBType.Timestamp, "TIMESTAMP '2024-02-29 13:45:30'").Should().Be(expected);
        Read<DateTimeOffset?>(connection, DuckDBType.Timestamp, "TIMESTAMP '2024-02-29 13:45:30'").Should().Be(expected);
        Read<DateTime>(connection, DuckDBType.Timestamp, "TIMESTAMP '2024-02-29 13:45:30'").Should().Be(expected.DateTime);
    }

    // Registers a table function with one parameter of the DuckDB type, calls it with the literal, and returns the
    // parameter as the function read it, as TRead.
    private static TRead Read<TRead>(DuckDBConnection connection, DuckDBType type, string literal)
    {
        var name = $"read_{Guid.NewGuid():N}";
        TRead read = default;

        connection.RegisterTableFunction(name, parameters =>
        {
            read = parameters[0].GetValue<TRead>();
            return new TableFunction([new ColumnInfo("x", typeof(int))], new[] { 1 });
        }, (item, writers, rowIndex) => writers[0].WriteValue((int)item, rowIndex), type);

        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT * FROM {name}({literal})";
        command.ExecuteScalar();

        return read;
    }

    private static DuckDBConnection Open()
    {
        var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        return connection;
    }
}
