using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Test;

// A typed scalar function reads a nullable argument and writes a nullable result through a line for its type. These
// tests run a function that returns its argument for every value type a function can take, over a column with
// values and NULLs, and let DuckDB compare what came back with what went in, NULLs included.
public class ScalarFunctionNullableTypesTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void ANullableArgumentAndResultRoundTripForEveryType()
    {
        Connection.ExecuteNonQuery("""
            CREATE OR REPLACE TABLE nullableFunctionValues AS
            SELECT
                CASE WHEN i % 3 = 0 THEN NULL ELSE i % 2 = 0 END AS b,
                CASE WHEN i % 3 = 0 THEN NULL ELSE (i - 50)::TINYINT END AS i8,
                CASE WHEN i % 3 = 0 THEN NULL ELSE (i * 100 - 5000)::SMALLINT END AS i16,
                CASE WHEN i % 3 = 0 THEN NULL ELSE (i * 1000003 - 50000000)::INTEGER END AS i32,
                CASE WHEN i % 3 = 0 THEN NULL ELSE (i * 100000000007 - 5000000000000)::BIGINT END AS i64,
                CASE WHEN i % 3 = 0 THEN NULL ELSE (i * 2)::UTINYINT END AS u8,
                CASE WHEN i % 3 = 0 THEN NULL ELSE (i * 600)::USMALLINT END AS u16,
                CASE WHEN i % 3 = 0 THEN NULL ELSE (i * 40000000)::UINTEGER END AS u32,
                CASE WHEN i % 3 = 0 THEN NULL ELSE i::UBIGINT * 180000000000000000::UBIGINT END AS u64,
                CASE WHEN i % 3 = 0 THEN NULL ELSE (i / 7)::FLOAT END AS f32,
                CASE WHEN i % 3 = 0 THEN NULL ELSE (i / 7)::DOUBLE END AS f64,
                CASE WHEN i % 3 = 0 THEN NULL ELSE (i / 7)::DECIMAL(38, 18) END AS dec,
                CASE WHEN i % 3 = 0 THEN NULL ELSE i::HUGEINT * 1701411834604692317316873037158841::HUGEINT END AS huge,
                CASE WHEN i % 3 = 0 THEN NULL ELSE ('00000000-0000-0000-0000-' || lpad(i::VARCHAR, 12, '0'))::UUID END AS id,
                CASE WHEN i % 3 = 0 THEN NULL ELSE TIMESTAMP '2024-01-01' + INTERVAL (i * 3601) SECOND END AS ts,
                CASE WHEN i % 3 = 0 THEN NULL ELSE INTERVAL (i) DAY + INTERVAL (i * 1000) MICROSECOND END AS iv,
                CASE WHEN i % 3 = 0 THEN NULL ELSE DATE '1990-01-01' + (i * 37)::INTEGER END AS d,
                CASE WHEN i % 3 = 0 THEN NULL ELSE TIME '00:00:00' + INTERVAL (i * 997) SECOND END AS t,
                CASE WHEN i % 3 = 0 THEN NULL ELSE TIMESTAMPTZ '2024-01-01 00:00:00+00' + INTERVAL (i * 3601) SECOND END AS tz
            FROM range(100) r(i)
            """);

        Check<bool>("b");
        Check<sbyte>("i8");
        Check<short>("i16");
        Check<int>("i32");
        Check<long>("i64");
        Check<byte>("u8");
        Check<ushort>("u16");
        Check<uint>("u32");
        Check<ulong>("u64");
        Check<float>("f32");
        Check<double>("f64");
        Check<decimal>("dec");
        Check<BigInteger>("huge");
        Check<Guid>("id");
        Check<DateTime>("ts");
        Check<TimeSpan>("iv");
        Check<DateOnly>("d");
        Check<TimeOnly>("t");
        Check<DuckDBDateOnly>("d");
        Check<DuckDBTimeOnly>("t");
        Check<DateTimeOffset>("tz");
    }

    // The form with a variable number of arguments writes its result in a loop of its own.
    [Fact]
    public void ANullableResultOfAFunctionWithManyArguments()
    {
        Connection.ExecuteNonQuery("""
            CREATE OR REPLACE TABLE nullableVarargsValues AS
            SELECT CASE WHEN i % 3 = 0 THEN NULL ELSE i::INTEGER END AS a, CASE WHEN i % 5 = 0 THEN NULL ELSE (i * 7)::INTEGER END AS b
            FROM range(100) r(i)
            """);

        Connection.RegisterScalarFunction<int?, int?>("sum_or_null", (int?[] values) => values.Any(value => value is null) ? null : values.Sum());

        Command.CommandText = "SELECT count(*) FILTER (WHERE sum_or_null(a, b) IS NULL), count(*) FILTER (WHERE sum_or_null(a, b) IS DISTINCT FROM a + b) FROM nullableVarargsValues";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetInt64(0).Should().Be(47);
        reader.GetInt64(1).Should().Be(0);
    }

    private void Check<T>(string column) where T : struct
    {
        var name = $"same_{typeof(T).Name.ToLowerInvariant()}_{column}";
        var calls = 0;
        Connection.RegisterScalarFunction<T?, T?>(name, value =>
        {
            calls++;
            return value;
        });

        Command.CommandText = $"SELECT count(*), count({column}), count(*) FILTER (WHERE {name}({column}) IS DISTINCT FROM {column}) FROM nullableFunctionValues";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetInt64(0).Should().Be(100, typeof(T).Name);
        reader.GetInt64(1).Should().Be(66, typeof(T).Name);
        reader.GetInt64(2).Should().Be(0, $"{typeof(T).Name} values came back different from {column}");
        calls.Should().Be(100, typeof(T).Name);
    }
}
