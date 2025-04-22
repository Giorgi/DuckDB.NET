using Bogus;
using DuckDB.NET.Native;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Numerics;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBManagedAppenderListTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void ListValuesBool()
    {
        ListValuesInternal("Bool", faker => faker.Random.Bool());
    }

    [Fact]
    public void ListValuesBoolNullable()
    {
        ListValuesInternal("Bool", faker => faker.Random.Bool().OrNull(faker));
    }

    [Fact]
    public void ListValuesSByte()
    {
        ListValuesInternal("TinyInt", faker => faker.Random.SByte());
    }

    [Fact]
    public void ListValuesSByteNullable()
    {
        ListValuesInternal("TinyInt", faker => faker.Random.SByte().OrNull(faker));
    }

    [Fact]
    public void ListValuesShort()
    {
        ListValuesInternal("SmallInt", faker => faker.Random.Short());
    }

    [Fact]
    public void ListValuesShortNullable()
    {
        ListValuesInternal("SmallInt", faker => faker.Random.Short().OrNull(faker));
    }

    [Fact]
    public void ListValuesInt()
    {
        ListValuesInternal("Integer", faker => faker.Random.Int());
    }

    [Fact]
    public void ListValuesIntNullable()
    {
        ListValuesInternal("Integer", faker => faker.Random.Int().OrNull(faker));
    }

    [Fact]
    public void ListValuesLong()
    {
        ListValuesInternal("BigInt", faker => faker.Random.Long());
    }

    [Fact]
    public void ListValuesLongNullable()
    {
        ListValuesInternal("BigInt", faker => faker.Random.Long().OrNull(faker));
    }

    [Fact]
    public void ListValuesHugeInt()
    {
        ListValuesInternal("HugeInt", faker => BigInteger.Subtract(DuckDBHugeInt.HugeIntMaxValue, faker.Random.Int(min: 0)));
    }

    [Fact]
    public void ListValuesByte()
    {
        ListValuesInternal("UTinyInt", faker => faker.Random.Byte());
    }

    [Fact]
    public void ListValuesByteNullable()
    {
        ListValuesInternal("UTinyInt", faker => faker.Random.Byte().OrNull(faker));
    }

    [Fact]
    public void ListValuesUShort()
    {
        ListValuesInternal("USmallInt", faker => faker.Random.UShort());
    }

    [Fact]
    public void ListValuesUShortNullable()
    {
        ListValuesInternal("USmallInt", faker => faker.Random.UShort().OrNull(faker));
    }

    [Fact]
    public void ListValuesUInt()
    {
        ListValuesInternal("UInteger", faker => faker.Random.UInt());
    }

    [Fact]
    public void ListValuesUIntNullable()
    {
        ListValuesInternal("UInteger", faker => faker.Random.UInt().OrNull(faker));
    }

    [Fact]
    public void ListValuesULong()
    {
        ListValuesInternal("UBigInt", faker => faker.Random.ULong());
    }

    [Fact]
    public void ListValuesULongNullable()
    {
        ListValuesInternal("UBigInt", faker => faker.Random.ULong().OrNull(faker));
    }

    [Fact]
    public void ListValuesUHugeInt()
    {
        ListValuesInternal("UHugeInt", faker => BigInteger.Subtract(DuckDBHugeInt.HugeIntMaxValue, faker.Random.Int(min: 0)));
    }

    [Fact]
    public void ListValuesDecimal()
    {
        ListValuesInternal("Decimal(38,28)", faker => faker.Random.Decimal());
    }

    [Fact]
    public void ListValuesDecimalNullable()
    {
        ListValuesInternal("Decimal(38,28)", faker => faker.Random.Decimal().OrNull(faker));
    }

    [Fact]
    public void ListValuesFloat()
    {
        ListValuesInternal("Float", faker => faker.Random.Float());
    }

    [Fact]
    public void ListValuesFloatNullable()
    {
        ListValuesInternal("Float", faker => faker.Random.Float().OrNull(faker));
    }

    [Fact]
    public void ListValuesDouble()
    {
        ListValuesInternal("Double", faker => faker.Random.Double());
    }

    [Fact]
    public void ListValuesDoubleNullable()
    {
        ListValuesInternal("Double", faker => faker.Random.Double().OrNull(faker));
    }

    [Fact]
    public void ListValuesGuid()
    {
        ListValuesInternal("UUID", faker => faker.Random.Guid());
    }

    [Fact]
    public void ListValuesGuidNullable()
    {
        ListValuesInternal("UUID", faker => faker.Random.Guid().OrNull(faker));
    }

    [Fact]
    public void ListValuesDate()
    {
        ListValuesInternal("Date", faker => faker.Date.Past().Date);
    }

    [Fact]
    public void ListValuesString()
    {
        ListValuesInternal("Varchar", faker => faker.Random.Utf16String());
    }

    [Fact]
    public void ListValuesInterval()
    {
        ListValuesInternal("Interval", faker =>
        {
            var timespan = faker.Date.Timespan();

            return TimeSpan.FromTicks(timespan.Ticks - timespan.Ticks % 10);
        });
    }

    [Fact]
    public void ArrayValuesInt()
    {
        ListValuesInternal("Integer", faker => faker.Random.Int(), 5);
    }

    [Fact]
    public void ListValuesEnum()
    {
        Command.CommandText = "CREATE TYPE test_enum AS ENUM('test1','test2','test3');";
        Command.ExecuteNonQuery();

        ListValuesInternal("test_enum", faker => faker.Random.CollectionItem([null, "test1", "test2", "test3"]));
        ListValuesInternal("test_enum", faker => faker.Random.CollectionItem<TestEnum?>([null, TestEnum.Test1, TestEnum.Test2, TestEnum.Test3]));
    }

    [Fact]
    public void ListStringAndGuid()
    {
        Command.CommandText = " CREATE TABLE test01(id uuid primary key, list text[]);";
        Command.ExecuteNonQuery();

        var textList = new List<string> { "TEST1", "TEST2" };
        using (var appender = Connection.CreateAppender("test01"))
        {
            for (var i = 1; i <= 10000; i++)
            {
                var id = Guid.NewGuid();

                appender.CreateRow().AppendValue(id).AppendValue(textList).EndRow();
            }
        }

        Command.CommandText = "Select list from test01";
        using var reader = Command.ExecuteReader();

        while (reader.Read())
        {
            var value = reader.GetFieldValue<List<string>>(0);
            value.Should().BeEquivalentTo(textList);
        }
    }

    [Fact]
    public void ListGuidAndGuid()
    {
        Command.CommandText = " CREATE TABLE test02(id uuid primary key, list uuid[]);";
        Command.ExecuteNonQuery();

        List<Guid> guids = [Guid.NewGuid(), Guid.NewGuid()];
        using (var appender = Connection.CreateAppender("test02"))
        {
            for (var i = 1; i <= 10000; i++)
            {
                var id = Guid.NewGuid();

                appender.CreateRow().AppendValue(id).AppendValue(guids).EndRow();
            }
        }

        Command.CommandText = "Select list from test02";
        using var reader = Command.ExecuteReader();

        while (reader.Read())
        {
            var value = reader.GetFieldValue<List<Guid>>(0);
            value.Should().BeEquivalentTo(guids);
        }
    }

    [Fact]
    public void ListDecimalAndGuid()
    {
        Command.CommandText = " CREATE TABLE test03(id uuid primary key, list decimal[]);";
        Command.ExecuteNonQuery();

        List<decimal> decimalList = [1m, 2m];
        using (var appender = Connection.CreateAppender("test03"))
        {
            for (var i = 1; i <= 10000; i++)
            {
                var id = Guid.NewGuid();

                appender.CreateRow().AppendValue(id).AppendValue(decimalList).EndRow();
            }
        }

        Command.CommandText = "Select list from test03";
        using var reader = Command.ExecuteReader();

        while (reader.Read())
        {
            var value = reader.GetFieldValue<List<decimal>>(0);
            value.Should().BeEquivalentTo(decimalList);
        }
    }

    private void ListValuesInternal<T>(string typeName, Func<Faker, T> generator, int? length = null)
    {
        var rows = 2000;
        var table = "managedAppenderLists";

        var columnLength = length.HasValue ? length.Value.ToString() : "";
        Command.CommandText = $"CREATE OR REPLACE TABLE {table} (a Integer, b {typeName}[{columnLength}], c {typeName}[][]);";
        Command.ExecuteNonQuery();

        var lists = new List<List<T>>();
        var nestedLists = new List<List<List<T>>>();

        for (var i = 0; i < rows; i++)
        {
            lists.Add(GetRandomList(generator, length ?? Random.Shared.Next(0, 200)));

            var item = new List<List<T>>();
            nestedLists.Add(item);

            for (var j = 0; j < Random.Shared.Next(0, 10); j++)
            {
                item.Add(GetRandomList(generator, Random.Shared.Next(0, 20)));
            }
        }

        using (var appender = Connection.CreateAppender(table))
        {
            for (var i = 0; i < rows; i++)
            {
                appender.CreateRow().AppendValue(i).AppendValue(lists[i]).AppendValue(nestedLists[i]).EndRow();
            }
        }

        Command.CommandText = $"SELECT * FROM {table} order by 1";
        using var reader = Command.ExecuteReader();

        var index = 0;
        while (reader.Read())
        {
            var list = reader.GetFieldValue<List<T>>(1);
            list.Should().BeEquivalentTo(lists[index]);

            var nestedList = reader.GetFieldValue<List<List<T>>>(2);
            nestedList.Should().BeEquivalentTo(nestedLists[index]);

            index++;
        }

        //Test for appending an array with wrong length
        if (length.HasValue)
        {
            var appender = Connection.CreateAppender(table);

            appender.Invoking(app => app.CreateRow().AppendValue(0).AppendValue(GetRandomList(generator, length + 1)))
                .Should().Throw<InvalidOperationException>().Where(exception => exception.Message.Contains(length.ToString()));

            appender.Invoking(app => app.CreateRow().AppendValue(0).AppendValue(GetRandomList(generator, length - 1)))
                .Should().Throw<InvalidOperationException>().Where(exception => exception.Message.Contains(length.ToString()));
        }
    }

    private enum TestEnum
    {
        Test1 = 0,
        Test2 = 1,
        Test3 = 2,
    }
}