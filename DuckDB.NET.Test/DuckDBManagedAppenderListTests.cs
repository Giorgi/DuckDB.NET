using System.Collections.Generic;
using System;
using Bogus;
using FluentAssertions;
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
    public void ListValuesSByte()
    {
        ListValuesInternal("TinyInt", faker => faker.Random.SByte());
    }

    [Fact]
    public void ListValuesShort()
    {
        ListValuesInternal("SmallInt", faker => faker.Random.Short());
    }

    [Fact]
    public void ListValuesInt()
    {
        ListValuesInternal("Integer", faker => faker.Random.Int());
    }

    [Fact]
    public void ListValuesLong()
    {
        ListValuesInternal("BigInt", faker => faker.Random.Long());
    }

    [Fact]
    public void ListValuesByte()
    {
        ListValuesInternal("UTinyInt", faker => faker.Random.Byte());
    }

    [Fact]
    public void ListValuesUShort()
    {
        ListValuesInternal("USmallInt", faker => faker.Random.UShort());
    }

    [Fact]
    public void ListValuesUInt()
    {
        ListValuesInternal("UInteger", faker => faker.Random.UInt());
    }

    [Fact]
    public void ListValuesULong()
    {
        ListValuesInternal("UBigInt", faker => faker.Random.ULong());
    }

    [Fact]
    public void ListValuesDecimal()
    {
        ListValuesInternal("Decimal(38,28)", faker => faker.Random.Decimal());
    }

    [Fact]
    public void ListValuesFloat()
    {
        ListValuesInternal("Float", faker => faker.Random.Float());
    }

    [Fact]
    public void ListValuesDouble()
    {
        ListValuesInternal("Double", faker => faker.Random.Double());
    }

    [Fact]
    public void ListValuesGuid()
    {
        ListValuesInternal("UUID", faker => faker.Random.Guid());
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
    public void ArrayValuesInt()
    {
        ListValuesInternal("Integer", faker => faker.Random.Int(), 5);
    }


    private void ListValuesInternal<T>(string typeName, Func<Faker, T> generator, int? length = null)
    {
        var rows = 2000;
        var table = $"managedAppenderLists";

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
        var reader = Command.ExecuteReader();

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
}