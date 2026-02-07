using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Dapper;
using FluentAssertions;
using Xunit;

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
        }, false, true);

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
        }, false);

        Command.CommandText = "CREATE TABLE big_table_3 AS SELECT (greatest(random(), 0.1) * 10000)::BIGINT i FROM range(100) t(i);";
        Command.ExecuteNonQuery();

        var longs = Connection.Query<long>("SELECT my_random_scalar(i) FROM big_table_3").ToList();
        longs.Should().BeEquivalentTo(values);
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
}