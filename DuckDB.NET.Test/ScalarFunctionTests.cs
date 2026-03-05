using System.Globalization;

namespace DuckDB.NET.Test;

public class ScalarFunctionTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void RegisterScalarFunctionWithVarargs()
    {
        var values = new List<long>();
        Connection.RegisterScalarFunction<long, long>("my_rand", (readers, writer, rowCount) =>
        {
            for (ulong index = 0; index < rowCount; index++)
            {
                var value = 0L;

                if (readers.Count == 0)
                {
                    value = Random.Shared.NextInt64();
                }

                if (readers.Count == 1)
                {
                    value = Random.Shared.NextInt64(readers[0].GetValue<long>(index));
                }

                if (readers.Count == 2)
                {
                    value = Random.Shared.NextInt64(readers[0].GetValue<long>(index), readers[1].GetValue<long>(index));
                }

                writer.WriteValue(value, index);

                values.Add(value);
            }
        }, new() { IsPureFunction = false }, @params: true);

        Command.CommandText = "CREATE TABLE big_table_1 AS SELECT (greatest(random(), 0.1) * 10000)::BIGINT i FROM range(10000) t(i);";
        Command.ExecuteNonQuery();

        var longs = Connection.Query<long>("SELECT my_rand() FROM big_table_1").ToList();
        longs.Should().BeEquivalentTo(values);

        values.Clear();

        longs = Connection.Query<long>("SELECT my_rand(i) FROM big_table_1").ToList();
        longs.Should().BeEquivalentTo(values);

        values.Clear();

        longs = Connection.Query<long>("SELECT my_rand(i, i+10) FROM big_table_1").ToList();
        longs.Should().BeEquivalentTo(values);
    }

    [Fact]
    public void RegisterScalarFunctionWithoutParameters()
    {
        var values = new List<long>();
        Connection.RegisterScalarFunction<long>("my_random", (writer, rowCount) =>
        {
            for (ulong index = 0; index < rowCount; index++)
            {
                var value = Random.Shared.NextInt64();

                writer.WriteValue(value, index);

                values.Add(value);
            }
        });

        Command.CommandText = "CREATE TABLE big_table_2 AS SELECT (greatest(random(), 0.1) * 10000)::BIGINT i FROM range(100) t(i);";
        Command.ExecuteNonQuery();

        var longs = Connection.Query<long>("SELECT my_random() FROM big_table_2").ToList();
        longs.Should().BeEquivalentTo(values);
    }

    [Fact]
    public void RegisterScalarFunctionWithOneParameter()
    {
        var values = new List<long>();
        Connection.RegisterScalarFunction<long, long>("my_random_scalar", (readers, writer, rowCount) =>
        {
            for (ulong index = 0; index < rowCount; index++)
            {
                var value = Random.Shared.NextInt64(readers[0].GetValue<long>(index));

                writer.WriteValue(value, index);

                values.Add(value);
            }
        }, new() { IsPureFunction = false });

        Command.CommandText = "CREATE TABLE big_table_3 AS SELECT (greatest(random(), 0.1) * 10000)::BIGINT i FROM range(100) t(i);";
        Command.ExecuteNonQuery();

        var longs = Connection.Query<long>("SELECT my_random_scalar(i) FROM big_table_3").ToList();
        longs.Should().BeEquivalentTo(values);
    }

    [Fact]
    public void RegisterScalarFunctionCallbackThrows()
    {
        const string functionName = "throwing_scalar";

        Connection.RegisterScalarFunction<long, long>(functionName, (_, _, _) =>
        {
            throw new InvalidOperationException("Scalar callback failed");
        });

        Connection.Invoking(con => con.Query<long>($"SELECT {functionName}(1)"))
                  .Should().Throw<DuckDBException>()
                  .WithMessage("*Scalar callback failed*");
    }

    [Fact]
    public void RegisterScalarFunctionIsPrime()
    {
        Connection.RegisterScalarFunction<int, bool>("is_prime", (readers, writer, rowCount) =>
        {
            for (ulong index = 0; index < rowCount; index++)
            {
                var value = readers[0].GetValue<int>(index);
                var prime = true;

                for (int i = 2; i <= Math.Sqrt(value); i++)
                {
                    if (value % i == 0)
                    {
                        prime = false;
                        break;
                    }
                }

                writer.WriteValue(prime, index);
            }
        });

        var primes = Connection.Query<int>("SELECT i FROM range(2, 100) t(i) where is_prime(i::INT)").ToList();
        primes.Should().BeEquivalentTo(new List<int>() { 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97 });
    }

    [Fact]
    public void RegisterScalarFunction()
    {
        var minValue = long.MaxValue;
        Connection.RegisterScalarFunction<long, long, long>("my_addition", (readers, writer, rowCount) =>
        {
            for (ulong index = 0; index < rowCount; index++)
            {
                var value = readers[0].GetValue<long>(index) + readers[1].GetValue<long>(index);
                writer.WriteValue(value, index);

                minValue = long.Min(minValue, value);
            }
        });

        Command.CommandText = "CREATE TABLE big_table AS SELECT (greatest(random(), 0.1) * 100)::BIGINT i FROM range(10000) t(i);";
        Command.ExecuteNonQuery();

        Command.CommandText = "SELECT MIN(my_addition(i, i)) FROM big_table;";
        var scalar = Command.ExecuteScalar();
        scalar.Should().Be(minValue);
    }

    [Fact]
    public void RegisterScalarFunctionWithAny()
    {
        Connection.RegisterScalarFunction<object, string, string>("to_string", (readers, writer, rowCount) =>
        {
            for (ulong index = 0; index < rowCount; index++)
            {
                var format = readers[1].GetValue<string>(index);

                var value = readers[0].GetValue(index);

                if (value is IFormattable formattable)
                {
                    writer.WriteValue(formattable.ToString(format, CultureInfo.InvariantCulture), index);
                }

                //switch (readers[0].DuckDBType)
                //{
                //    case DuckDBType.Integer:
                //        writer.WriteValue(readers[0].GetValue<int>(index).ToString(format, CultureInfo.InvariantCulture), index);
                //        break;
                //    case DuckDBType.Date:
                //        writer.WriteValue(readers[0].GetValue<DateOnly>(index).ToString(format, CultureInfo.InvariantCulture), index);
                //        break;
                //    case DuckDBType.Double:
                //        writer.WriteValue(readers[0].GetValue<double>(index).ToString(format, CultureInfo.InvariantCulture), index);
                //        break;
                //    default:
                //        writer.WriteValue(readers[0].GetValue(index).ToString(), index);
                //        break;
                //}
            }
        });

        Command.CommandText = "CREATE TABLE TestTableAnyType (a Integer, b Date, c double)";
        Command.ExecuteNonQuery();

        var randomList = GetRandomList(faker => new { a = faker.Random.Int(), b = DateOnly.FromDateTime(faker.Date.Past()), c = faker.Random.Double() });

        using (var appender = Connection.CreateAppender("TestTableAnyType"))
        {
            foreach (var item in randomList)
            {
                appender.CreateRow().AppendValue(item.a).AppendValue((DateOnly?)item.b).AppendValue(item.c).EndRow();
            }
        }

        Command.CommandText = "SELECT a, to_string(a, 'G'), b, to_string(b, 'dd-MM-yyyy'), c, to_string(c, 'G'), FROM TestTableAnyType;";

        var rows = Connection.Query<(int a, string formatA, DateOnly b, string formatB, double c, string formatC)>(Command.CommandText).ToList();
        rows.Should().BeEquivalentTo(randomList.Select(item => (item.a, item.a.ToString("G", CultureInfo.InvariantCulture), item.b, item.b.ToString("dd-MM-yyyy", CultureInfo.InvariantCulture), item.c, item.c.ToString("G", CultureInfo.InvariantCulture))));
    }

    [Fact]
    public void SimplifiedScalarFunctionZeroParams()
    {
        Connection.RegisterScalarFunction("the_answer", () => 42);

        var results = Connection.Query<int>("SELECT the_answer() FROM range(5)").ToList();
        results.Should().AllBeEquivalentTo(42);
        results.Should().HaveCount(5);
    }

    [Fact]
    public void SimplifiedScalarFunctionOneParam()
    {
        Connection.RegisterScalarFunction<int, bool>("is_prime_simple", IsPrime);

        var primes = Connection.Query<int>("SELECT i FROM range(2, 100) t(i) WHERE is_prime_simple(i::INT)").ToList();
        primes.Should().BeEquivalentTo([2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97]);
    }

    [Fact]
    public void SimplifiedScalarFunctionTwoParams()
    {
        Connection.RegisterScalarFunction<long, long, long>("add_simple", (a, b) => a + b);

        Command.CommandText = "SELECT add_simple(10, 32)";
        var result = Command.ExecuteScalar();
        result.Should().Be(42L);
    }

    [Fact]
    public void SimplifiedScalarFunctionThreeParams()
    {
        Connection.RegisterScalarFunction<int, int, int, int>("clamp_simple", (value, min, max) => Math.Clamp(value, min, max));

        var results = Connection.Query<int>("SELECT clamp_simple(i::INT, 3, 7) FROM range(10) t(i)").ToList();
        results.Should().BeEquivalentTo([3, 3, 3, 3, 4, 5, 6, 7, 7, 7]);
    }

    [Fact]
    public void SimplifiedScalarFunctionNullPropagation()
    {
        Connection.RegisterScalarFunction<int, int>("double_it", x => x * 2);

        Command.CommandText = "SELECT double_it(NULL::INT)";
        var result = Command.ExecuteScalar();
        result.Should().Be(DBNull.Value);
    }

    [Fact]
    public void SimplifiedScalarFunctionNullPropagationMultipleParams()
    {
        Connection.RegisterScalarFunction<int, int, int>("add_ints", (a, b) => a + b);

        Command.CommandText = "SELECT add_ints(5, NULL::INT)";
        var result = Command.ExecuteScalar();
        result.Should().Be(DBNull.Value);

        Command.CommandText = "SELECT add_ints(NULL::INT, 5)";
        result = Command.ExecuteScalar();
        result.Should().Be(DBNull.Value);
    }

    [Fact]
    public void SimplifiedScalarFunctionStringReturn()
    {
        Connection.RegisterScalarFunction<int, string>("to_words", n => n switch
        {
            1 => "one",
            2 => "two",
            3 => "three",
            _ => null
        });

        var results = Connection.Query<string>("SELECT to_words(i::INT) FROM range(1, 5) t(i)").ToList();
        results.Should().BeEquivalentTo(["one", "two", "three", null]);
    }

    [Fact]
    public void SimplifiedScalarFunctionNullableValueTypeReturn()
    {
        Connection.RegisterScalarFunction<int, int?>("maybe_double", n => n > 3 ? n * 2 : null);

        var results = Connection.Query<int?>("SELECT maybe_double(i::INT) FROM range(1, 6) t(i)").ToList();
        results.Should().BeEquivalentTo([(int?)null, null, null, 8, 10]);
    }

    [Fact]
    public void SimplifiedScalarFunctionVarargsSum()
    {
        Connection.RegisterScalarFunction("hi_sum", (long[] args) => args.Sum());

        Connection.Query<long>("SELECT hi_sum(1, 2, 3)").Single().Should().Be(6);
        Connection.Query<long>("SELECT hi_sum(10)").Single().Should().Be(10);
        Connection.Query<long>("SELECT hi_sum()").Single().Should().Be(0);
    }

    [Fact]
    public void SimplifiedScalarFunctionVarargsConcat()
    {
        Connection.RegisterScalarFunction("hi_concat", (string[] parts) => string.Join(", ", parts));

        Connection.Query<string>("SELECT hi_concat('a', 'b', 'c')").Single().Should().Be("a, b, c");
        Connection.Query<string>("SELECT hi_concat('only')").Single().Should().Be("only");
    }

    [Fact]
    public void SimplifiedScalarFunctionVarargsMethodGroup()
    {
        Connection.RegisterScalarFunction<long, long>("hi_max", Max);

        Connection.Query<long>("SELECT hi_max(3, 7, 1, 9, 2)").Single().Should().Be(9);

        static long Max(params long[] values) => values.Max();
    }

    [Fact]
    public void SimplifiedScalarFunctionVarargsBranchOnCount()
    {
        var values = new List<long>();
        Connection.RegisterScalarFunction("hi_rand", (long[] args) =>
        {
            var value = args.Length switch
            {
                0 => Random.Shared.NextInt64(),
                1 => Random.Shared.NextInt64(args[0]),
                _ => Random.Shared.NextInt64(args[0], args[1])
            };

            values.Add(value);
            return value;
        }, options: new() { IsPureFunction = false });

        Command.CommandText = "CREATE TABLE varargs_table AS SELECT (greatest(random(), 0.1) * 10000)::BIGINT i FROM range(10000) t(i);";
        Command.ExecuteNonQuery();

        var longs = Connection.Query<long>("SELECT hi_rand() FROM varargs_table").ToList();
        longs.Should().BeEquivalentTo(values);

        values.Clear();

        longs = Connection.Query<long>("SELECT hi_rand(i) FROM varargs_table").ToList();
        longs.Should().BeEquivalentTo(values);

        values.Clear();

        longs = Connection.Query<long>("SELECT hi_rand(i, i+10) FROM varargs_table").ToList();
        longs.Should().BeEquivalentTo(values);
    }

    [Fact]
    public void SimplifiedScalarFunctionVarargsLargeChunk()
    {
        Connection.RegisterScalarFunction("hi_sum_large", (long[] args) => args.Sum());

        var results = Connection.Query<long>("SELECT hi_sum_large(i, i * 2) FROM range(5000) t(i)").ToList();
        results.Should().HaveCount(5000);
        results[0].Should().Be(0);
        results[1].Should().Be(3);
        results[100].Should().Be(300);
    }

    [Fact]
    public void SimplifiedScalarFunctionAnyOneParam()
    {
        Connection.RegisterScalarFunction<object, string>("net_type_name", value => value.GetType().Name);

        Connection.Query<string>("SELECT net_type_name(42)").Single().Should().Be("Int32");
        Connection.Query<string>("SELECT net_type_name(42::BIGINT)").Single().Should().Be("Int64");
        Connection.Query<string>("SELECT net_type_name('hello')").Single().Should().Be("String");
        Connection.Query<string>("SELECT net_type_name('2024-01-01'::DATE)").Single().Should().Be("DateOnly");
    }

    [Fact]
    public void SimplifiedScalarFunctionAnyTwoParams()
    {
        Connection.RegisterScalarFunction("format_net",
            (object value, string format) => value is IFormattable f
                ? f.ToString(format, CultureInfo.InvariantCulture)
                : value?.ToString() ?? "");

        Connection.Query<string>("SELECT format_net(255, 'X')").Single().Should().Be("FF");
        Connection.Query<string>("SELECT format_net(0.15, 'P')").Single().Should().Be("15.00 %");
        Connection.Query<string>("SELECT format_net('2024-11-06'::DATE, 'yyyy/MM/dd')").Single().Should().Be("2024/11/06");
    }

    [Fact]
    public void SimplifiedScalarFunctionAnyThreeParams()
    {
        Connection.RegisterScalarFunction("format_net_culture",
            (object value, string format, string culture) => value is IFormattable f
                ? f.ToString(format, CultureInfo.GetCultureInfo(culture))
                : value?.ToString() ?? "");

        var deResult = Connection.Query<string>("SELECT format_net_culture(1234.5, 'N2', 'de-DE')").Single();
        deResult.Should().Be(1234.5.ToString("N2", CultureInfo.GetCultureInfo("de-DE")));

        var usResult = Connection.Query<string>("SELECT format_net_culture(1234.5, 'N2', 'en-US')").Single();
        usResult.Should().Be("1,234.50");
    }

    [Fact]
    public void SimplifiedScalarFunctionVarargsAny()
    {
        Connection.RegisterScalarFunction("concat_any", (object[] args) => string.Join(", ", args));

        Connection.Query<string>("SELECT concat_any(42, 'hello', true)").Single().Should().Be("42, hello, True");
        Connection.Query<string>("SELECT concat_any(true, '2024-01-01'::DATE)").Single()
            .Should().Be($"True, {new DateOnly(2024, 1, 1)}");
        Connection.Query<string>("SELECT concat_any('only')").Single().Should().Be("only");
    }

    [Fact]
    public void ScalarFunctionHandlesNulls_LowLevel()
    {
        Connection.RegisterScalarFunction<int, string>("null_or_val", (readers, writer, rowCount) =>
        {
            for (ulong i = 0; i < rowCount; i++)
            {
                if (!readers[0].IsValid(i))
                    writer.WriteValue("was_null", i);
                else
                    writer.WriteValue(readers[0].GetValue<int>(i).ToString(), i);
            }
        }, new() { HandlesNulls = true });

        Command.CommandText = "SELECT null_or_val(NULL::INT)";
        var result = Command.ExecuteScalar();
        result.Should().Be("was_null");

        Command.CommandText = "SELECT null_or_val(1234)";
        result = Command.ExecuteScalar();
        result.Should().Be("1234");
    }

    [Fact]
    public void SimplifiedScalarFunctionHandlesNulls_NullableParam()
    {
        Connection.RegisterScalarFunction<int?, string>("describe_val",
            x => x.HasValue ? x.Value.ToString() : "nothing",
            new() { HandlesNulls = true });

        Command.CommandText = "SELECT describe_val(NULL::INT)";
        var result = Command.ExecuteScalar();
        result.Should().Be("nothing");
    }

    [Fact]
    public void SimplifiedScalarFunctionHandlesNulls_MixedParams()
    {
        Connection.RegisterScalarFunction<int?, int, string>("coalesce_add",
            (a, b) => a.HasValue ? (a.Value + b).ToString() : b.ToString(),
            new() { HandlesNulls = true });

        Command.CommandText = "SELECT coalesce_add(NULL::INT, 5)";
        var result = Command.ExecuteScalar();
        result.Should().Be("5");
    }

    [Fact]
    public void SimplifiedScalarFunctionHandlesNulls_AllNonNullable_Throws()
    {
        var act = () => Connection.RegisterScalarFunction("bad",
            (Func<int, string>)(x => x.ToString()), new() { HandlesNulls = true });

        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void SimplifiedScalarFunctionHandlesNulls_NonNullableReceivesNull_Throws()
    {
        Connection.RegisterScalarFunction<int?, int, string>("bad_mix",
            (a, b) => $"{a},{b}",
            new() { HandlesNulls = true });

        Command.CommandText = "SELECT bad_mix(1, NULL::INT)";
        var act = () => Command.ExecuteScalar();
        act.Should().Throw<DuckDBException>().WithMessage("*received NULL*");
    }

    [Fact]
    public void SimplifiedScalarFunctionHandlesNulls_StringParam()
    {
        Connection.RegisterScalarFunction<string, string>("echo_or_default",
            s => s ?? "was_null",
            new() { HandlesNulls = true });

        Command.CommandText = "SELECT echo_or_default(NULL::VARCHAR)";
        var result = Command.ExecuteScalar();
        result.Should().Be("was_null");

        Command.CommandText = "SELECT echo_or_default('hello')";
        result = Command.ExecuteScalar();
        result.Should().Be("hello");
    }

    [Fact]
    public void SimplifiedScalarFunctionHandlesNulls_Varargs()
    {
        Connection.RegisterScalarFunction<int?, string>("sum_or_null",
            args =>
            {
                if (args.Any(a => !a.HasValue)) return "has_null";
                return args.Sum(a => a!.Value).ToString();
            },
            new() { HandlesNulls = true });

        Command.CommandText = "SELECT sum_or_null(1, NULL::INT, 3)";
        var result = Command.ExecuteScalar();
        result.Should().Be("has_null");

        Command.CommandText = "SELECT sum_or_null(1, 2, 3)";
        result = Command.ExecuteScalar();
        result.Should().Be("6");
    }

    private static bool IsPrime(int value)
    {
        for (int i = 2; i <= Math.Sqrt(value); i++)
        {
            if (value % i == 0) return false;
        }
        return true;
    }
}