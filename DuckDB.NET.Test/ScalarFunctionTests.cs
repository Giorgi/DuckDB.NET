using Dapper;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
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
            for (int index = 0; index < rowCount; index++)
            {
                var value = 0L;

                if (readers.Length == 0)
                {
                    value = Random.Shared.NextInt64();
                }

                if (readers.Length == 1)
                {
                    value = Random.Shared.NextInt64(readers[0].GetValue<long>((ulong)index));
                }

                if (readers.Length == 2)
                {
                    value = Random.Shared.NextInt64(readers[0].GetValue<long>((ulong)index), readers[1].GetValue<long>((ulong)index));
                }

                writer.AppendValue(value, index);

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
        Connection.RegisterScalarFunction<long>("my_random", (readers, writer, rowCount) =>
        {
            for (int index = 0; index < rowCount; index++)
            {
                var value = Random.Shared.NextInt64();

                writer.AppendValue(value, index);

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
            for (int index = 0; index < rowCount; index++)
            {
                var value = Random.Shared.NextInt64(readers[0].GetValue<long>((ulong)index));

                writer.AppendValue(value, index);

                values.Add(value);
            }
        });

        Command.CommandText = "CREATE TABLE big_table_3 AS SELECT (greatest(random(), 0.1) * 10000)::BIGINT i FROM range(100) t(i);";
        Command.ExecuteNonQuery();

        var longs = Connection.Query<long>("SELECT my_random_scalar(i) FROM big_table_3").ToList();
        longs.Should().BeEquivalentTo(values);
    }

    [Fact]
    public void RegisterScalarFunction()
    {
        var minValue = long.MaxValue;
        Connection.RegisterScalarFunction<long, long, long>("my_addition", (readers, writer, rowCount) =>
        {
            for (int index = 0; index < rowCount; index++)
            {
                var value = readers[0].GetValue<long>((ulong)index) + readers[1].GetValue<long>((ulong)index);
                writer.AppendValue(value, index);

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
            for (int index = 0; index < rowCount; index++)
            {
                var format = readers[1].GetValue<string>((ulong)index);

                var value = readers[0].GetValue((ulong)index);

                if (value is IFormattable formattable)
                {
                    writer.AppendValue(formattable.ToString(format, CultureInfo.InvariantCulture), index);
                }

                //switch (readers[0].DuckDBType)
                //{
                //    case DuckDBType.Integer:
                //        writer.AppendValue(readers[0].GetValue<int>((ulong)index).ToString(format, CultureInfo.InvariantCulture), index);
                //        break;
                //    case DuckDBType.Date:
                //        writer.AppendValue(readers[0].GetValue<DateOnly>((ulong)index).ToString(format, CultureInfo.InvariantCulture), index);
                //        break;
                //    case DuckDBType.Double:
                //        writer.AppendValue(readers[0].GetValue<double>((ulong)index).ToString(format, CultureInfo.InvariantCulture), index);
                //        break;
                //    default:
                //        writer.AppendValue(readers[0].GetValue((ulong)index).ToString(), index);
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