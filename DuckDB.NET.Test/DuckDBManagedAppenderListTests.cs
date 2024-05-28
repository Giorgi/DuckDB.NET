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
    public void ListValuesString()
    {
        ListValuesInternal("Varchar", faker => faker.Random.Utf16String());
    }

    [Fact]
    public void ListValuesSByte()
    {
        ListValuesInternal("TinyInt", faker => faker.Random.SByte());
    }

    [Fact]
    public void ListValuesInt()
    {
        ListValuesInternal("Integer", faker => faker.Random.Int());
    }

    [Fact]
    public void ArrayValuesInt()
    {
        ListValuesInternal("Integer", faker => faker.Random.Int(), 5);
    }

    [Fact]
    public void ListValuesLong()
    {
        ListValuesInternal("BigInt", faker => faker.Random.Long());
    }


    public void ListValuesInternal<T>(string typeName, Func<Faker, T> generator, int? length = null)
    {
        var rows = 2000;
        var table = $"managedAppender{typeName}Lists";

        var columnLength = length.HasValue ? length.Value.ToString() : "";
        Command.CommandText = $"CREATE TABLE IF NOT EXISTS {table} (a Integer, b {typeName}[{columnLength}], c {typeName}[][]);";
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