using Bogus;
using DuckDB.NET.Data;
using DuckDB.NET.Native;
using FluentAssertions;
using FluentAssertions.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class ListParameterTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    private void TestInsertSelect<T>(string duckDbType, Func<Faker, T> generator, int? length = null)
    {
        var list = GetRandomList(generator, length ?? Random.Shared.Next(10, 200));
        var nestedList = new List<List<T>> { GetRandomList(generator, 5), GetRandomList(generator, 10), GetRandomList(generator, 20) };

        Command.CommandText = $"CREATE OR REPLACE TABLE ParameterListTest (a {duckDbType}[], b {duckDbType}[10], c {duckDbType}[][]);";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO ParameterListTest (a, b, c) VALUES ($list, $array, $nestedList);";
        Command.Parameters.Add(new DuckDBParameter(list));
        Command.Parameters.Add(new DuckDBParameter(list.Take(10).ToList()));
        Command.Parameters.Add(new DuckDBParameter(nestedList));
        Command.ExecuteNonQuery();

        Command.CommandText = "SELECT * FROM ParameterListTest;";

        using var reader = Command.ExecuteReader();
        reader.Read();

        var value = reader.GetFieldValue<List<T>>(0);
        value.Should().BeEquivalentTo(list);

        var arrayValue = reader.GetFieldValue<List<T>>(1);
        arrayValue.Should().BeEquivalentTo(list.Take(10));

        var nestedListValue = reader.GetFieldValue<List<List<T>>>(2);
        nestedListValue.Should().BeEquivalentTo(nestedList);

        Command.CommandText = "DROP TABLE ParameterListTest";
        Command.ExecuteNonQuery();
    }

    [Fact]
    public void CanBindBoolList()
    {
        TestInsertSelect("bool", faker => faker.Random.Bool().OrNull(faker));
    }

    [Fact]
    public void CanBindSByteList()
    {
        TestInsertSelect("tinyint", faker => faker.Random.SByte().OrNull(faker));
    }

    [Fact]
    public void CanBindShortList()
    {
        TestInsertSelect("SmallInt", faker => faker.Random.Short().OrNull(faker));
    }

    [Fact]
    public void CanBindIntegerList()
    {
        TestInsertSelect("int", faker => faker.Random.Int().OrNull(faker));
    }

    [Fact]
    public void CanBindLongList()
    {
        TestInsertSelect("BigInt", faker => faker.Random.Long().OrNull(faker));
    }

    [Fact]
    public void CanBindHugeIntList()
    {
        TestInsertSelect("HugeInt", faker => BigInteger.Subtract(DuckDBHugeInt.HugeIntMaxValue, faker.Random.Int(min: 0)).OrNull(faker));
    }

    [Fact]
    public void CanBindByteList()
    {
        TestInsertSelect("UTinyInt", faker => faker.Random.Byte().OrNull(faker));
    }

    [Fact]
    public void CanBindUShortList()
    {
        TestInsertSelect("USmallInt", faker => faker.Random.UShort().OrNull(faker));
    }

    [Fact]
    public void CanBindUIntList()
    {
        TestInsertSelect("UInteger", faker => faker.Random.UInt().OrNull(faker));
    }

    [Fact]
    public void CanBindULongList()
    {
        TestInsertSelect("UBigInt", faker => faker.Random.ULong().OrNull(faker));
    }

    [Fact]
    public void CanBindFloatList()
    {
        TestInsertSelect("Float", faker => faker.Random.Float().OrNull(faker));
    }

    [Fact]
    public void CanBindDoubleList()
    {
        TestInsertSelect("Double", faker => faker.Random.Double().OrNull(faker));
    }

    [Fact]
    public void CanBindDecimalList()
    {
        TestInsertSelect("Decimal(38, 28)", faker => faker.Random.Decimal().OrNull(faker));
    }

    [Fact]
    public void CanBindGuidList()
    {
        TestInsertSelect("UUID", faker => faker.Random.Uuid().OrNull(faker));
    }

    [Fact]
    public void CanBindDateTimeList()
    {
        TestInsertSelect("Date", faker => faker.Date.Past().Date);
    }

    [Fact]
    public void CanBindDateTimeOffsetList()
    {
        TestInsertSelect("TimeTZ", faker =>
        {
            var dateTime = faker.Date.Between(DateTime.Now.AddYears(-100), DateTime.Now.AddYears(100));

            if (dateTime.Hour < 1)
            {
                dateTime = dateTime.AddHours(1);
            }

            dateTime = dateTime.AddTicks(-dateTime.Ticks % 10);

            return dateTime.ToDateTimeOffset(TimeSpan.FromHours(1));
        });
    }

    [Fact]
    public void CanBindStringList()
    {
        TestInsertSelect("String", faker => faker.Random.Utf16String().OrNull(faker));
    }

    [Fact]
    public void CanBindIntervalList()
    {
        TestInsertSelect("Interval", faker =>
        {
            var timespan = faker.Date.Timespan();

            return TimeSpan.FromTicks(timespan.Ticks - timespan.Ticks % 10);
        });
    }

    [Fact]
    public void CanBindDuckDBDateOnlyList()
    {
        TestInsertSelect("Date", faker => ((DuckDBDateOnly)faker.Date.Past().Date).OrNull(faker));
    }

    [Fact]
    public void CanBindDuckDBTimeOnlyList()
    {
        TestInsertSelect("Time", faker => ((DuckDBTimeOnly)faker.Date.Past()).OrNull(faker));
    }

    [Fact]
    public void CanBindDateOnlyList()
    {
        TestInsertSelect("Date", faker => DateOnly.FromDateTime(faker.Date.Past().Date).OrNull(faker));
    }

    [Fact]
    public void CanBindTimeOnlyList()
    {
        TestInsertSelect("Time", faker =>
        {
            //Truncate nanoseconds because duckdb doesn't store it.
            var dateTime = faker.Date.Past();
            return new TimeOnly(dateTime.TimeOfDay.Ticks - dateTime.TimeOfDay.Ticks % 10);
        });
    }
}