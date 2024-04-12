using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBDataReaderMapTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void ReadMap()
    {
        Command.CommandText = "SELECT MAP { 'key1': 1, 'key2': 5, 'key3': 7 }";
        var reader = Command.ExecuteReader();
        reader.GetFieldType(0).Should().Be(typeof(Dictionary<string, int>));

        reader.Read();
        var value = reader.GetValue(0);

        value.Should().BeOfType<Dictionary<string, int>>();

        var expectation = new Dictionary<string, int>() { { "key1", 1 }, { "key2", 5 }, { "key3", 7 } };
        value.Should().BeEquivalentTo(expectation);
    }

    [Fact]
    public void ReadMapTwoRows()
    {
        Command.CommandText = "Select * from (SELECT MAP { 'key1': 1, 'key2': 5, 'key3': 7 } Union SELECT MAP { 'key2': 15, 'key24': 7 }) order by 1";
        var reader = Command.ExecuteReader();

        reader.Read();
        var value = reader.GetValue(0);

        value.Should().BeOfType<Dictionary<string, int>>();

        var expectation = new Dictionary<string, int>() { { "key1", 1 }, { "key2", 5 }, { "key3", 7 } };
        value.Should().BeEquivalentTo(expectation);

        reader.Read();
        value = reader.GetValue(0);


        expectation = new Dictionary<string, int>() { { "key2", 15 }, { "key24", 7 } };
        value.Should().BeEquivalentTo(expectation);
    }

    [Fact]
    public void ReadMapStronglyTyped()
    {
        Command.CommandText = "SELECT MAP { 'key1': 1, 'key2': 5, 'key3': 7 }";
        var reader = Command.ExecuteReader();

        reader.Read();
        var value = reader.GetFieldValue<Dictionary<string, int>>(0);

        var expectation = new Dictionary<string, int>() { { "key1", 1 }, { "key2", 5 }, { "key3", 7 } };
        value.Should().BeEquivalentTo(expectation);
    }

    [Fact]
    public void ReadMapWithNullInNullableDictionary()
    {
        Command.CommandText = "SELECT MAP { 'key1': 1, 'key2': NULL, 'key3': 7 }";
        var reader = Command.ExecuteReader();

        reader.Read();
        var value = reader.GetFieldValue<Dictionary<string, int?>>(0);

        var expectation = new Dictionary<string, int?>() { { "key1", 1 }, { "key2", null }, { "key3", 7 } };
        value.Should().BeEquivalentTo(expectation);
    }

    [Fact]
    public void ReadMapWithNullInReferenceTypeDictionary()
    {
        Command.CommandText = "SELECT MAP { 'key1': 'abc', 'key2': NULL, 'key3': 'ghi' }";
        var reader = Command.ExecuteReader();

        reader.Read();
        var value = reader.GetFieldValue<Dictionary<string, string>>(0);

        var expectation = new Dictionary<string, string>() { { "key1", "abc" }, { "key2", null }, { "key3", "ghi" } };
        value.Should().BeEquivalentTo(expectation);
    }

    [Fact]
    public void ReadMapWithNullInNotNullableDictionaryThrowsException()
    {
        Command.CommandText = "SELECT MAP { 'key1': 1, 'key2': NULL, 'key3': 7 }";
        var reader = Command.ExecuteReader();

        reader.Read();
        reader.Invoking(r => r.GetFieldValue<Dictionary<string, int>>(0)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void ReadMapOfList()
    {
        Command.CommandText = "SELECT MAP { ['a', 'b']: [1.1, 2.2], ['c', 'd']: [3.3, 4.4] };";
        var reader = Command.ExecuteReader();

        reader.Read();

        var value = reader.GetValue(0) as Dictionary<List<string>, List<decimal>>;

        var expectation = new Dictionary<List<string>, List<decimal>>(new ListEqualityClass())
        {
            { new List<string>() { "a", "b" }, new List<decimal> { 1.1m, 2.2m } },
            { new List<string>() { "c", "d" }, new List<decimal> { 3.3m, 4.4m } },
        };

        foreach (var (key, decimals) in value)
        {
            expectation[key].Should().BeEquivalentTo(decimals);
        }
    }

    [Fact]
    public void ReadMapWrongTypeThrowsException()
    {
        Command.CommandText = "SELECT MAP { ['a', 'b']: [1.1, 2.2], ['c', 'd']: [3.3, 4.4] };";
        var reader = Command.ExecuteReader();

        reader.Read();
        reader.Invoking(r => r.GetFieldValue<List<KeyValuePair<string, int>>>(0)).Should().Throw<InvalidOperationException>();
    }

    class ListEqualityClass : IEqualityComparer<List<string>>
    {
        public bool Equals(List<string> x, List<string> y)
        {
            return x.SequenceEqual(y);
        }

        public int GetHashCode(List<string> obj)
        {
            return string.Join(",", obj).GetHashCode();
        }
    }
}