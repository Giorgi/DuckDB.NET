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

        Connection.RegisterScalarFunction<long, long>(functionName, (_, _, _) => throw new InvalidOperationException("Scalar callback failed"));

        Connection.Invoking(con => con.Query<long>($"SELECT {functionName}(1)"))
                  .Should().Throw<DuckDBException>()
                  .WithMessage("*Scalar callback failed*");
    }

    [Fact]
    public void RegisterScalarFunctionCallbackThrowsInLaterStreamingChunk()
    {
        using var connection = OpenConnectionForLateStreamingErrors();
        connection.RegisterScalarFunction<long, long>("throwing_late_scalar",
            x => x < 500_000 ? x : throw new InvalidOperationException("Scalar callback failed late"));

        using var command = connection.CreateCommand();
        command.UseStreamingMode = true;
        command.CommandText = "SELECT throwing_late_scalar(i) FROM range(1000000) t(i)";

        using (var reader = command.ExecuteReader())
        {
            var rows = 0;
            var act = () =>
            {
                while (reader.Read())
                {
                    rows++;
                }
            };

            act.Should().Throw<DuckDBException>()
               .WithInnerException<InvalidOperationException>()
               .WithMessage("Scalar callback failed late");
            rows.Should().BeGreaterThan(0);
        }

        // The stored callback exception was consumed, so it must not leak into the next error on this connection.
        command.UseStreamingMode = false;
        command.CommandText = "SELECT CAST('not a number' AS INTEGER)";

        command.Invoking(c => c.ExecuteScalar())
               .Should().Throw<DuckDBException>()
               .Where(e => e.InnerException == null);
    }

    [Fact]
    public void RegisterScalarFunctionWithDuckDBDateOnlyAndTimeOnly()
    {
        Connection.RegisterScalarFunction<DuckDBDateOnly, DuckDBDateOnly>("same_duckdb_date", date => date);
        Connection.RegisterScalarFunction<DuckDBTimeOnly, DuckDBTimeOnly>("same_duckdb_time", time => time);

        Command.CommandText = """
                              SELECT same_duckdb_date(DATE '2024-05-17')::VARCHAR, same_duckdb_date('infinity'::DATE)::VARCHAR,
                                     same_duckdb_time(TIME '13:45:30.123456')::VARCHAR,
                                     typeof(same_duckdb_date(DATE '2024-05-17')), typeof(same_duckdb_time(TIME '13:45:30'))
                              """;
        using var reader = Command.ExecuteReader();

        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("2024-05-17");
        reader.GetString(1).Should().Be("infinity");
        reader.GetString(2).Should().Be("13:45:30.123456");
        reader.GetString(3).Should().Be("DATE");
        reader.GetString(4).Should().Be("TIME");
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
        results.Should().BeEquivalentTo("one", "two", "three", null);
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
        Connection.RegisterScalarFunction<long, long>("hi_rand", (readers, writer, rowCount) =>
        {
            for (ulong i = 0; i < rowCount; i++)
            {
                var value = readers.Count switch
                {
                    0 => Random.Shared.NextInt64(),
                    1 => Random.Shared.NextInt64(readers[0].GetValue<long>(i)),
                    _ => Random.Shared.NextInt64(readers[0].GetValue<long>(i), readers[1].GetValue<long>(i))
                };

                writer.WriteValue(value, i);
                values.Add(value);
            }
        }, new() { IsPureFunction = false }, @params: true);

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
    public void ScalarFunctionHandlesNulls_LowLevel_StringNullWithGetValue()
    {
        Connection.RegisterScalarFunction<string, string>("echo_nullable", (readers, writer, rowCount) =>
        {
            for (ulong i = 0; i < rowCount; i++)
            {
                var value = readers[0].GetValue<string>(i);
                writer.WriteValue(value ?? "was_null", i);
            }
        }, new() { HandlesNulls = true });

        Command.CommandText = "SELECT echo_nullable(NULL::VARCHAR)";
        var result = Command.ExecuteScalar();
        result.Should().Be("was_null");

        Command.CommandText = "SELECT echo_nullable('hello')";
        result = Command.ExecuteScalar();
        result.Should().Be("hello");
    }

    [Fact]
    public void SimplifiedScalarFunctionHandlesNulls_NullableParam()
    {
        Connection.RegisterScalarFunction<int?, string>("describe_val", x => x.HasValue ? x.Value.ToString() : "nothing");

        Command.CommandText = "SELECT describe_val(NULL::INT)";
        var result = Command.ExecuteScalar();
        result.Should().Be("nothing");
    }

    [Fact]
    public void SimplifiedScalarFunctionHandlesNulls_MixedParams()
    {
        Connection.RegisterScalarFunction<int?, int, string>("coalesce_add",
            (a, b) => a.HasValue ? (a.Value + b).ToString() : b.ToString());

        Command.CommandText = "SELECT coalesce_add(NULL::INT, 5)";
        var result = Command.ExecuteScalar();
        result.Should().Be("5");
    }

    [Fact]
    public void SimplifiedScalarFunctionHandlesNulls_AllNonNullable_DefaultPropagation()
    {
        Connection.RegisterScalarFunction("non_nullable",
            (Func<int, string>)(x => x.ToString()));

        // Non-nullable param and a constant NULL: DuckDB answers NULL itself and never calls the function
        Command.CommandText = "SELECT non_nullable(NULL::INT)";
        var result = Command.ExecuteScalar();
        result.Should().Be(DBNull.Value);
    }

    // For a column, DuckDB calls a function without special handling with the whole chunk, NULL rows included. A row
    // with a NULL argument gets NULL and the function is not called for it, as for a constant. 5000 rows span three
    // chunks.
    [Fact]
    public void SimplifiedScalarFunctionNonNullableParamsGiveNullForANullInAColumn()
    {
        var calls = 0;
        Connection.RegisterScalarFunction<int, int>("plus_one_col", x => { System.Threading.Interlocked.Increment(ref calls); return x + 1; });

        var rows = Connection.Query<(long I, int? Result)>(
            "SELECT i, plus_one_col(CASE WHEN i % 7 = 0 THEN NULL ELSE i::INT END) FROM range(5000) t(i) ORDER BY i").ToList();

        rows.Should().HaveCount(5000);
        rows.Should().OnlyContain(row => row.Result == (row.I % 7 == 0 ? null : (int?)(row.I + 1)));
        calls.Should().Be(5000 - 715);
    }

    // A reference type parameter that is not declared nullable is not called with null either. (Nullable reference
    // types are off in this project, so the parameter is not declared nullable.)
    [Fact]
    public void SimplifiedScalarFunctionNonNullableStringParamIsNotCalledForANullInAColumn()
    {
        Connection.RegisterScalarFunction<string, string>("upper_col", s => s.ToUpperInvariant());

        var rows = Connection.Query<string>(
            "SELECT upper_col(CASE WHEN i = 1 THEN NULL ELSE 'v' || i END) FROM range(3) t(i) ORDER BY i").ToList();

        rows.Should().Equal("V0", null, "V2");
    }

    // A list result after a NULL row must still have the right offsets. 5000 rows span three chunks.
    [Fact]
    public void SimplifiedScalarFunctionNonNullableListParamGivesNullForANullListInAColumn()
    {
        Connection.RegisterScalarFunction<List<int>, List<int>>("twice_col", list => [.. list, .. list]);

        var rows = Connection.Query<(long I, List<int> Result)>(
            "SELECT i, twice_col(CASE WHEN i % 3 = 0 THEN NULL ELSE [i::INT] END) FROM range(5000) t(i) ORDER BY i").ToList();

        rows.Should().HaveCount(5000);
        rows.Should().OnlyContain(row => row.I % 3 == 0 ? row.Result == null : row.Result.SequenceEqual(new[] { (int)row.I, (int)row.I }));
    }

    [Fact]
    public void SimplifiedScalarFunctionNonNullableParamsGiveNullForANullInAnyColumn()
    {
        Connection.RegisterScalarFunction<int, int, int>("add_col", (a, b) => a + b);
        Connection.RegisterScalarFunction<int, int, int, int, int>("add4_col", (a, b, c, d) => a + b + c + d);
        Connection.RegisterScalarFunction<int, int>("sum_col", args => args.Sum());

        var rows = Connection.Query<(int? Two, int? Four, int? Varargs)>(
            "SELECT add_col(a, b), add4_col(a, b, 1, b), sum_col(a, b, 1) " +
            "FROM (VALUES (1, 1, 2), (2, NULL, 2), (3, 1, NULL), (4, NULL, NULL)) t(n, a, b) ORDER BY n").ToList();

        rows.Should().Equal((3, 6, 4), (null, null, null), (null, null, null), (null, null, null));
    }

    [Fact]
    public void SimplifiedScalarFunctionHandlesNulls_NonNullableReceivesNull_Throws()
    {
        Connection.RegisterScalarFunction<int?, int, string>("bad_mix",
            (a, b) => $"{a},{b}");

        Command.CommandText = "SELECT bad_mix(1, NULL::INT)";
        var act = () => Command.ExecuteScalar();
        act.Should().Throw<DuckDBException>().WithMessage("*received NULL*");
    }

#nullable enable
    [Fact]
    public void SimplifiedScalarFunctionHandlesNulls_NullableStringParam()
    {
        Connection.RegisterScalarFunction<string?, string>("echo_or_default",
            s => s ?? "was_null");

        Command.CommandText = "SELECT echo_or_default(NULL::VARCHAR)";
        var result = Command.ExecuteScalar();
        result.Should().Be("was_null");

        Command.CommandText = "SELECT echo_or_default('hello')";
        result = Command.ExecuteScalar();
        result.Should().Be("hello");
    }

    [Fact]
    public void SimplifiedScalarFunctionHandlesNulls_NonNullableStringParam_DefaultPropagation()
    {
        Connection.RegisterScalarFunction<string, string>("echo_non_null",
            s => s.ToUpper());

        // Non-nullable string param: DuckDB auto-propagates NULL
        Command.CommandText = "SELECT echo_non_null(NULL::VARCHAR)";
        var result = Command.ExecuteScalar();
        result.Should().Be(DBNull.Value);
    }
#nullable restore

    [Fact]
    public void SimplifiedScalarFunctionHandlesNulls_Varargs()
    {
        Connection.RegisterScalarFunction<int?, string>("sum_or_null",
            args =>
            {
                if (args.Any(a => !a.HasValue)) return "has_null";
                return args.Sum(a => a!.Value).ToString();
            });

        Command.CommandText = "SELECT sum_or_null(1, NULL::INT, 3)";
        var result = Command.ExecuteScalar();
        result.Should().Be("has_null");

        Command.CommandText = "SELECT sum_or_null(1, 2, 3)";
        result = Command.ExecuteScalar();
        result.Should().Be("6");
    }

#nullable enable
    [Fact]
    public void SimplifiedScalarFunctionHandlesNulls_NullableStringVarargs()
    {
        Connection.RegisterScalarFunction<string?, string>("coalesce_join",
            args => string.Join(", ", args.Select(a => a ?? "<null>")));

        Command.CommandText = "SELECT coalesce_join('a', NULL::VARCHAR, 'c')";
        var result = Command.ExecuteScalar();
        result.Should().Be("a, <null>, c");

        Command.CommandText = "SELECT coalesce_join('x', 'y')";
        result = Command.ExecuteScalar();
        result.Should().Be("x, y");
    }
#nullable restore

    [Fact]
    public void SimplifiedScalarFunctionHandlesNulls_NonNullableObjectReceivesNull_Throws()
    {
        Connection.RegisterScalarFunction<object, int?, string>("obj_mix", (obj, n) => $"{obj},{n}");

        Command.CommandText = "SELECT obj_mix(NULL::INT, 5)";
        var act = () => Command.ExecuteScalar();
        act.Should().Throw<DuckDBException>().WithMessage("*received NULL*");
    }

    [Fact]
    public void ScalarFunctionError_PreservesInnerException()
    {
        var originalException = new InvalidOperationException("custom scalar error");

        Connection.RegisterScalarFunction<long, long>("scalar_inner_err", (_, _, _) => throw originalException);

        var act = () => Connection.Query<long>("SELECT scalar_inner_err(1)");
        var ex = act.Should().Throw<DuckDBException>().Which;

        ex.Message.Should().Contain("custom scalar error");
        ex.InnerException.Should().BeOfType<InvalidOperationException>();
        ex.InnerException!.Message.Should().Be("custom scalar error");
        ex.InnerException.Should().BeSameAs(originalException);
    }

    // A function that returns a nullable value type writes it without boxing it, through one line per type in
    // VectorDataWriterBase.WriteNullable. Every type that has a line is here, with a value and a NULL, over more
    // rows than one chunk holds.
    [Fact]
    public void SimplifiedScalarFunctionReturnsNullableOfEveryType()
    {
        ReturnsNullable<bool>(true);
        ReturnsNullable<sbyte>(-5);
        ReturnsNullable<short>(-1234);
        ReturnsNullable<int>(-123456);
        ReturnsNullable<long>(-12345678901);
        ReturnsNullable<byte>(200);
        ReturnsNullable<ushort>(60000);
        ReturnsNullable<uint>(4000000000);
        ReturnsNullable<ulong>(18000000000000000000);
        ReturnsNullable<float>(1.5f);
        ReturnsNullable<double>(2.25);
        ReturnsNullable<decimal>(12.5m);
        ReturnsNullable<BigInteger>(BigInteger.Pow(2, 100));
        ReturnsNullable<Guid>(Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e"));
        ReturnsNullable<DateTime>(new DateTime(2024, 2, 29, 12, 30, 45));
        ReturnsNullable<TimeSpan>(new TimeSpan(1, 2, 3, 4));
        ReturnsNullable<DuckDBDateOnly>(new DuckDBDateOnly(2024, 2, 29));
        ReturnsNullable<DuckDBTimeOnly>(new DuckDBTimeOnly(12, 30, 45));
        ReturnsNullable<DateOnly>(new DateOnly(2024, 2, 29));
        ReturnsNullable<TimeOnly>(new TimeOnly(12, 30, 45));
        ReturnsNullable<DateTimeOffset>(new DateTimeOffset(2024, 2, 29, 12, 30, 45, TimeSpan.Zero));
    }

    private void ReturnsNullable<T>(T value) where T : struct
    {
        const int rowCount = 5000;
        var name = "nullable_" + typeof(T).Name.ToLowerInvariant();

        Connection.RegisterScalarFunction<int, T?>(name, number => number % 3 == 0 ? null : value);

        Command.CommandText = $"SELECT {name}(i::INT) FROM range({rowCount}) t(i) ORDER BY i";
        using var reader = Command.ExecuteReader();

        for (var row = 0; row < rowCount; row++)
        {
            reader.Read().Should().BeTrue();

            if (row % 3 == 0)
            {
                reader.IsDBNull(0).Should().BeTrue($"row {row} of {name} is NULL");
            }
            else if (!reader.GetFieldValue<T>(0).Equals(value))
            {
                Assert.Fail($"Row {row} of {name}: expected {value}, read {reader.GetFieldValue<T>(0)}");
            }
        }
    }

    // DuckDB copies the expression of a filter, and the function has to know its connection in the copy as well.
    [Theory]
    [InlineData("where", "SELECT i FROM range(5) t(i) WHERE {0}(i) > 0")]
    [InlineData("having", "SELECT i FROM range(5) t(i) GROUP BY i HAVING {0}(i) > 0")]
    public void ScalarFunctionError_PreservesInnerExceptionInAFilter(string place, string sql)
    {
        var originalException = new InvalidOperationException("custom filter error");

        // A name of its own for each case: the cases share one connection, and a name is registered once.
        var name = "scalar_err_in_" + place;
        Connection.RegisterScalarFunction<long, long>(name, (_, _, _) => throw originalException);

        Command.CommandText = string.Format(sql, name);
        var exception = Command.Invoking(command => command.ExecuteScalar()).Should().Throw<DuckDBException>().Which;

        exception.Message.Should().Contain("custom filter error");
        exception.InnerException.Should().BeSameAs(originalException);
    }

    // The typed forms each have their own loop over the rows. The tests below give every parameter a type of its
    // own and more rows than one chunk holds, so that a loop that reads the wrong column or the wrong row shows.
    [Fact]
    public void SimplifiedScalarFunctionOneParamOverSeveralChunks()
    {
        Connection.RegisterScalarFunction<int, long>("chunks_one", value => value * 3L + 1);

        var rows = Connection.Query<(int Input, long Result)>("SELECT i::INT, chunks_one(i::INT) FROM range(5000) t(i) ORDER BY i").ToList();

        rows.Should().HaveCount(5000);
        rows.Should().OnlyContain(row => row.Result == row.Input * 3L + 1);
    }

    [Fact]
    public void SimplifiedScalarFunctionTwoParamsOverSeveralChunks()
    {
        Connection.RegisterScalarFunction<int, string, string>("chunks_two", (number, text) => $"{number}:{text}");

        var rows = Connection.Query<(int Input, string Result)>("SELECT i::INT, chunks_two(i::INT, 'row ' || (i * 7)) FROM range(5000) t(i) ORDER BY i").ToList();

        rows.Should().HaveCount(5000);
        rows.Should().OnlyContain(row => row.Result == $"{row.Input}:row {row.Input * 7}");
    }

    [Fact]
    public void SimplifiedScalarFunctionThreeParamsOfDifferentTypes()
    {
        Connection.RegisterScalarFunction<int, long?, double, string>("three_mixed",
            (number, optional, ratio) => $"{number}|{optional?.ToString() ?? "none"}|{ratio.ToString(CultureInfo.InvariantCulture)}");

        var rows = Connection.Query<(int Input, string Result)>(
            "SELECT i::INT, three_mixed(i::INT, CASE WHEN i % 3 = 0 THEN NULL ELSE i * 1000000000 END, i + 0.5) FROM range(5000) t(i) ORDER BY i").ToList();

        rows.Should().HaveCount(5000);
        rows.Should().OnlyContain(row => row.Result == $"{row.Input}|{(row.Input % 3 == 0 ? "none" : (row.Input * 1000000000L).ToString())}|{row.Input}.5");

        // The second parameter takes a NULL. The third does not, and says so.
        Command.CommandText = "SELECT three_mixed(1, NULL, 2.5)";
        Command.ExecuteScalar().Should().Be("1|none|2.5");

        Command.CommandText = "SELECT three_mixed(1, 2, NULL)";
        Command.Invoking(command => command.ExecuteScalar()).Should().Throw<DuckDBException>().WithMessage("*'Double' received NULL*");
    }

    [Fact]
    public void SimplifiedScalarFunctionFourParams()
    {
        Connection.RegisterScalarFunction<int, long, string, double, string>("four_mixed",
            (number, large, text, ratio) => $"{number}|{large}|{text}|{ratio.ToString(CultureInfo.InvariantCulture)}");

        var rows = Connection.Query<(int Input, string Result)>(
            "SELECT i::INT, four_mixed(i::INT, i * 1000000000, 'row ' || (i * 7), i + 0.25) FROM range(5000) t(i) ORDER BY i").ToList();

        rows.Should().HaveCount(5000);
        rows.Should().OnlyContain(row => row.Result == $"{row.Input}|{row.Input * 1000000000L}|row {row.Input * 7}|{row.Input}.25");

        // No parameter takes a NULL, so DuckDB gives NULL for a constant NULL without calling the function.
        Command.CommandText = "SELECT four_mixed(1, 2, NULL, 4.5)";
        Command.ExecuteScalar().Should().Be(DBNull.Value);
    }

    [Fact]
    public void SimplifiedScalarFunctionFourParamsWithNulls()
    {
        Connection.RegisterScalarFunction<int?, long, double?, short, string>("four_nulls",
            (first, second, third, fourth) => $"{first?.ToString() ?? "none"}|{second}|{third?.ToString(CultureInfo.InvariantCulture) ?? "none"}|{fourth}");

        var rows = Connection.Query<(int Input, string Result)>(
            "SELECT i::INT, four_nulls(CASE WHEN i % 2 = 0 THEN NULL ELSE i::INT END, i * 1000000000, CASE WHEN i % 3 = 0 THEN NULL ELSE i + 0.5 END, (i % 100)::SMALLINT) " +
            "FROM range(5000) t(i) ORDER BY i").ToList();

        rows.Should().HaveCount(5000);
        rows.Should().OnlyContain(row => row.Result ==
            $"{(row.Input % 2 == 0 ? "none" : row.Input.ToString())}|{row.Input * 1000000000L}|{(row.Input % 3 == 0 ? "none" : row.Input + ".5")}|{row.Input % 100}");

        // The second and the fourth parameter do not take a NULL. The arguments are read in order, so with a NULL in
        // both it is the second that is reported.
        Command.CommandText = "SELECT four_nulls(1, 2, 3.5, NULL)";
        Command.Invoking(command => command.ExecuteScalar()).Should().Throw<DuckDBException>().WithMessage("*'Int16' received NULL*");

        Command.CommandText = "SELECT four_nulls(1, NULL, 3.5, NULL)";
        Command.Invoking(command => command.ExecuteScalar()).Should().Throw<DuckDBException>().WithMessage("*'Int64' received NULL*");
    }

    // A function with parameters gives the same result for the same arguments, as far as DuckDB is told, and one
    // without parameters does not. DuckDB calls the first kind once for a constant argument.
    [Fact]
    public void SimplifiedScalarFunctionsWithParametersArePure()
    {
        Connection.RegisterScalarFunction("stable_zero", () => 1);
        Connection.RegisterScalarFunction<int, int>("stable_one", a => a);
        Connection.RegisterScalarFunction<int, int, int>("stable_two", (a, b) => a + b);
        Connection.RegisterScalarFunction<int, int, int, int>("stable_three", (a, b, c) => a + b + c);
        Connection.RegisterScalarFunction<int, int, int, int, int>("stable_four", (a, b, c, d) => a + b + c + d);
        Connection.RegisterScalarFunction("stable_params", (int[] values) => values.Sum());

        var stability = Connection.Query<(string Name, string Stability)>(
            "SELECT function_name, stability FROM duckdb_functions() WHERE function_name LIKE 'stable\\_%' ESCAPE '\\' ORDER BY function_name").ToList();

        stability.Should().Equal(
            ("stable_four", "CONSISTENT"),
            ("stable_one", "CONSISTENT"),
            ("stable_params", "CONSISTENT"),
            ("stable_three", "CONSISTENT"),
            ("stable_two", "CONSISTENT"),
            ("stable_zero", "VOLATILE"));
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