using System;
using System.Collections.Generic;
using System.Linq;
using Dapper;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBDataReaderListTests : DuckDBTestBase
{
    public DuckDBDataReaderListTests(DuckDBDatabaseFixture db) : base(db)
    {
    }

    [Fact]
    public void ReadListOfIntegers()
    {
        Command.CommandText = "SELECT [1, 2, 3];";
        using var reader = Command.ExecuteReader();
        reader.Read();
        var list = reader.GetFieldValue<List<int>>(0);
        list.Should().BeEquivalentTo(new List<int> { 1, 2, 3 });
    }

    [Fact]
    public void ReadMultipleListOfIntegers()
    {
        Command.CommandText = "Select * from ( SELECT [1, 2, 3] Union Select [4, 5] Union Select []) order by 1";
        using var reader = Command.ExecuteReader();
        reader.Read();
        var list = reader.GetFieldValue<List<int>>(0);
        list.Should().BeEquivalentTo(new List<int>());

        reader.Read();
        list = reader.GetFieldValue<List<int>>(0);
        list.Should().BeEquivalentTo(new List<int> { 1, 2, 3 });

        reader.Read();
        list = reader.GetFieldValue<List<int>>(0);
        list.Should().BeEquivalentTo(new List<int> { 4, 5 });
    }

    [Fact]
    public void ReadListOfIntegersWithNulls()
    {
        Command.CommandText = "Select * from ( SELECT [1, 2, NULL, 3, NULL] Union Select [NULL, NULL, 4, 5] Union Select null) order by 1";
        using var reader = Command.ExecuteReader();
        reader.Read();
        var list = reader.GetFieldValue<List<int?>>(0);
        list.Should().BeEquivalentTo(new List<int?> { 1, 2, null, 3, null });

        reader.Read();
        reader.Invoking(rd => rd.GetFieldValue<List<int>>(0)).Should().Throw<NullReferenceException>();

        reader.Read();
        reader.IsDBNull(0).Should().BeTrue();
    }

    [Fact]
    public void ReadMultipleListOfDoubles()
    {
        Command.CommandText = "Select * from ( SELECT [1/2, 3/2, 5/2] Union Select [4, 5] Union Select []) order by 1";
        using var reader = Command.ExecuteReader();
        reader.Read();
        var list = reader.GetFieldValue<List<double>>(0);
        list.Should().BeEquivalentTo(new List<double>());

        reader.Read();
        list = reader.GetFieldValue<List<double>>(0);
        list.Should().BeEquivalentTo(new List<double> { 0.5, 1.5, 2.5 });

        reader.Read();
        list = reader.GetFieldValue<List<double>>(0);
        list.Should().BeEquivalentTo(new List<double> { 4, 5 });
    }

    [Fact]
    public void ReadMultipleListOfStrings()
    {
        Command.CommandText = "Select * from ( SELECT ['hello', NULL, 'world'] Union Select ['from DuckDB.Net', 'client'] Union Select []) order by 1";
        using var reader = Command.ExecuteReader();
        reader.Read();
        var list = reader.GetFieldValue<List<string>>(0);
        list.Should().BeEquivalentTo(new List<string>());

        reader.Read();
        list = reader.GetFieldValue<List<string>>(0);
        list.Should().BeEquivalentTo(new List<string> { "from DuckDB.Net", "client" });

        reader.Read();
        list = reader.GetFieldValue<List<string>>(0);
        list.Should().BeEquivalentTo(new List<string> { "hello", null, "world" });
    }

    [Fact]
    public void ReadMultipleListOfDecimals()
    {
        Command.CommandText = "Select * from ( SELECT [1.1, 2.3456, NULL] Union Select [73.56725, 264387.632673487236]) order by 1";
        var reader = Command.ExecuteReader();

        reader.Read();
        var list = reader.GetFieldValue<List<decimal?>>(0);
        list.Should().BeEquivalentTo(new List<decimal?> { 1.1m, 2.3456m, null });

        reader.Read();
        var value = reader.GetValue(0);
        value.Should().BeEquivalentTo(new List<decimal?> { 73.56725m, 264387.632673487236m });
        reader.Dispose();

        Command.CommandText = "SELECT [1.1, 2.34] ";
        reader = Command.ExecuteReader();

        reader.Read();
        list = reader.GetFieldValue<List<decimal?>>(0);
        list.Should().BeEquivalentTo(new List<decimal?> { 1.1m, 2.34m });
        reader.Dispose();
    }

    [Fact]
    public void ReadListOfTimeStamps()
    {
        Command.CommandText = "SELECT range(date '1992-01-01', date '1992-08-01', interval '1' month);";

        using var reader = Command.ExecuteReader();
        reader.Read();

        var list = reader.GetFieldValue<List<DateTime>>(0);
        list.Should().BeEquivalentTo(Enumerable.Range(0, 7).Select(m => new DateTime(1992, 1, 1).AddMonths(m)));
    }

    [Fact]
    public void ReadListOfDates()
    {
        Command.CommandText = "SELECT [Date '2002-04-06', Date '2008-10-12']";

        using var reader = Command.ExecuteReader();
        reader.Read();

        var list = reader.GetFieldValue<List<DateTime>>(0);
        list.Should().BeEquivalentTo(new List<DateTime> { new(2002, 4, 6), new(2008, 10, 12) });

        var dateList = reader.GetFieldValue<List<DateOnly>>(0);
        dateList.Should().BeEquivalentTo(new List<DateOnly> { new(2002, 4, 6), new(2008, 10, 12) });

        var nullableList = reader.GetFieldValue<List<DateTime?>>(0);
        nullableList.Should().BeEquivalentTo(new List<DateTime> { new(2002, 4, 6), new(2008, 10, 12) });
    }

    [Fact]
    public void ReadListOfTimes()
    {
        Command.CommandText = "SELECT [Time '12:14:16', Time '18:10:12']";

        using var reader = Command.ExecuteReader();
        reader.Read();

        var list = reader.GetFieldValue<List<TimeOnly>>(0);
        list.Should().BeEquivalentTo(new List<TimeOnly> { new(12, 14, 16), new(18, 10, 12) });
    }

    [Fact]
    public void ReadListWithDapper()
    {
        Command.CommandText = "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');";
        Command.ExecuteNonQuery();

        var person = Connection.QueryFirst<Person>("SELECT [1, 2, 3] as Ids, 'happy' as Mood");

        person.Ids.Should().BeEquivalentTo(new List<int> { 1, 2, 3 });
        person.Mood.Should().Be(DuckDBDataReaderEnumTests.Mood.Happy);
    }

    [Fact]
    public void ReadListAsEveryNumericType()
    {
        Command.CommandText = "SELECT [1, 2, 3];";
        using var reader = Command.ExecuteReader();
        reader.Read();

        TestReadValueAs<byte>();
        TestReadValueAs<sbyte>();
        TestReadValueAs<ushort>();
        TestReadValueAs<short>();
        TestReadValueAs<uint>();
        TestReadValueAs<int>();
        TestReadValueAs<ulong>();
        TestReadValueAs<long>();

        void TestReadValueAs<T>()
        {
            var list = reader.GetFieldValue<List<T>>(0);
            list.Should().BeEquivalentTo(new List<long> { 1, 2, 3 });
        }
    }

    [Fact]
    public void ReadListWithLargeValuesAsEveryNumericTypeThrowsException()
    {
        Command.CommandText = $"SELECT [{long.MaxValue - 1}, {long.MaxValue}];";
        using var reader = Command.ExecuteReader();
        reader.Read();

        TestReadValueAs<byte>();
        TestReadValueAs<sbyte>();
        TestReadValueAs<ushort>();
        TestReadValueAs<short>();
        TestReadValueAs<uint>();
        TestReadValueAs<int>();

        void TestReadValueAs<T>()
        {
            reader.Invoking(dataReader => dataReader.GetFieldValue<List<T>>(0)).Should().Throw<InvalidCastException>();
        }
    }

    class Person
    {
        public List<int> Ids { get; set; }
        public DuckDBDataReaderEnumTests.Mood Mood { get; set; }
    }
}