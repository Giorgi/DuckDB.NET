using Dapper;
using FluentAssertions;
using System;
using System.Collections.Generic;
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
        }, true);

        Command.CommandText = "CREATE TABLE big_table_1 AS SELECT (greatest(random(), 0.1) * 10000)::BIGINT i FROM range(10000) t(i);";
        Command.ExecuteNonQuery();

        var longs = Connection.Query<long>("SELECT my_rand(i) FROM big_table_1").ToList();
        longs.Should().BeEquivalentTo(values);

        values.Clear();

        longs = Connection.Query<long>("SELECT my_rand(i, i+10) FROM big_table_1").ToList();
        longs.Should().BeEquivalentTo(values);
    }

    [Fact]
    public void RegisterScalarFunctionWithOneParameter()
    {
        var values = new List<long>();
        Connection.RegisterScalarFunction<long, long>("my_random", (readers, writer, rowCount) =>
        {
            for (int index = 0; index < rowCount; index++)
            {
                var value = Random.Shared.NextInt64(readers[0].GetValue<long>((ulong)index));

                writer.AppendValue(value, index);

                values.Add(value);
            }
        });

        Command.CommandText = "CREATE TABLE big_table_2 AS SELECT (greatest(random(), 0.1) * 10000)::BIGINT i FROM range(100) t(i);";
        Command.ExecuteNonQuery();

        var longs = Connection.Query<long>("SELECT my_random(i) FROM big_table_2").ToList();
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
}