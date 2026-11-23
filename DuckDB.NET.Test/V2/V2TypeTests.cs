using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Test.V2;

// Stage 2, step 2 of docs/V2-MIGRATION.md: the readers that need more than the data of a vector. V2 has no getter per
// kind, so DECIMAL's width and scale and an ENUM's names come from the type's parameters, and the storage of both
// follows from them as V2 states in its contract. These cases sit on each storage boundary.
[Trait("V2", "Ready")]
public class V2TypeTests
{
    private static DuckDBConnection Open()
    {
        var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        return connection;
    }

    [Theory]
    [InlineData(4, 2, "12.34")]                                   // int16
    [InlineData(5, 2, "-123.45")]                                 // int32
    [InlineData(9, 3, "123456.789")]                              // int32
    [InlineData(10, 3, "-1234567.891")]                           // int64
    [InlineData(18, 4, "12345678901234.5678")]                    // int64
    [InlineData(19, 4, "-123456789012345.6789")]                  // int128
    [InlineData(38, 10, "1234567890123456789.0123456789")]        // int128
    public void DecimalIsReadAtEveryStorageWidth(int width, int scale, string text)
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT '{text}'::DECIMAL({width},{scale}) AS d, NULL::DECIMAL({width},{scale}) AS n";

        using var reader = command.ExecuteReader();
        reader.Read().Should().BeTrue();

        reader.GetDecimal(0).Should().Be(decimal.Parse(text, System.Globalization.CultureInfo.InvariantCulture));
        reader.IsDBNull(1).Should().BeTrue();

        var schema = reader.GetSchemaTable();
        schema.Rows[0]["NumericPrecision"].Should().Be((byte)width);
        schema.Rows[0]["NumericScale"].Should().Be((byte)scale);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(255)]     // uint8
    [InlineData(256)]     // uint16
    [InlineData(65535)]   // uint16
    [InlineData(65536)]   // uint32
    public void EnumIsReadAtEveryStorageWidth(int size)
    {
        using var connection = Open();
        connection.ExecuteNonQuery($"CREATE TYPE sized AS ENUM (SELECT 'e' || i FROM range({size}) t(i))");

        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT ('e' || i)::sized AS e FROM range({size}) t(i) WHERE i IN (0, 1, {size - 1}) ORDER BY i";

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("e0");
        reader.GetFieldValue<uint>(0).Should().Be(0);

        reader.Read().Should().BeTrue();
        reader.GetValue(0).Should().Be("e1");

        if (size > 2)
        {
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be($"e{size - 1}");
            reader.GetFieldValue<long>(0).Should().Be(size - 1);
        }

        reader.Read().Should().BeFalse();
    }

    [Fact]
    public void TemporalAndWideIntegerTypesAreRead()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:;TimeZone=UTC");
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT DATE '2024-02-29' AS d, TIME '12:34:56.789' AS t, TIMESTAMP '2024-02-29 12:34:56.789' AS ts, " +
                              "TIMESTAMPTZ '2024-02-29 12:34:56+00' AS tz, INTERVAL 3 DAYS AS i, " +
                              "'00000000-0000-0000-0000-000000000001'::UUID AS u, 170141183460469231731687303715884105727::HUGEINT AS h";

        using var reader = command.ExecuteReader();
        reader.Read().Should().BeTrue();

        reader.GetDateTime(0).Should().Be(new DateTime(2024, 2, 29));
        reader.GetFieldValue<TimeOnly>(1).Should().Be(new TimeOnly(12, 34, 56, 789));
        reader.GetDateTime(2).Should().Be(new DateTime(2024, 2, 29, 12, 34, 56, 789));
        reader.GetFieldValue<DateTimeOffset>(3).Should().Be(new DateTimeOffset(2024, 2, 29, 12, 34, 56, TimeSpan.Zero));
        reader.GetFieldValue<TimeSpan>(4).Should().Be(TimeSpan.FromDays(3));
        reader.GetGuid(5).Should().Be(new Guid("00000000-0000-0000-0000-000000000001"));
        reader.GetFieldValue<BigInteger>(6).Should().Be(BigInteger.Parse("170141183460469231731687303715884105727"));
    }

    [Fact]
    public void AColumnWithNoReaderDoesNotStopTheQuery()
    {
        // TIMESTAMP_TZ_NS has no reader, as with V1: the query runs, the other columns are read, and reading that
        // column throws.
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT 1 AS one, now()::TIMESTAMPTZ_NS AS tzns";

        using var reader = command.ExecuteReader();
        reader.Read().Should().BeTrue();

        reader.GetInt32(0).Should().Be(1);
        reader.GetDataTypeName(1).Should().Be("42");
        reader.Invoking(r => r.GetValue(1)).Should().Throw<Exception>();
    }
}
