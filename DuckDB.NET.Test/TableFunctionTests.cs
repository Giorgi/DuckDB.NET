using System;
using Dapper;
using DuckDB.NET.Data;
using FluentAssertions;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Octokit;
using Xunit;

namespace DuckDB.NET.Test;

[Experimental("DuckDBNET001")]
public class TableFunctionTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void RegisterTableFunctionWithOneParameter()
    {
        Connection.RegisterTableFunction<int>("demo", async (parameters) =>
        {
            var value = parameters[0].GetValue<int>();

            return new TableFunction(new List<ColumnInfo>()
            {
                new ColumnInfo("foo", typeof(int)),
            }, Enumerable.Range(0, value));
        }, (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((int)item, rowIndex);
        });

        var data = Connection.Query<int>("SELECT * FROM demo(30);");
        data.Should().BeEquivalentTo(Enumerable.Range(0, 30));
    }

    [Fact]
    public void RegisterTableFunctionWithTwoParameters()
    {
        Connection.RegisterTableFunction<string, int>("github_search", async (parameters) =>
        {
            var term = parameters[0].GetValue<string>();
            var stars = parameters[1].GetValue<int>();

            var client = new GitHubClient(new ProductHeaderValue("DuckDB-Table-Valued-Function"));

            var request = new SearchRepositoriesRequest(term)
            {
                Stars = new Octokit.Range(stars, SearchQualifierOperator.GreaterThan)
            };

            var result = await client.Search.SearchRepo(request);

            return new TableFunction(new List<ColumnInfo>()
            {
                new ColumnInfo("name", typeof(string)),
                new ColumnInfo("description", typeof(string)),
                new ColumnInfo("stargazers", typeof(int)),
                new ColumnInfo("url", typeof(string)),
                new ColumnInfo("owner", typeof(string)),
            }, result.Items);
        }, (item, writers, rowIndex) =>
        {
            var repo = (Repository)item;
            writers[0].WriteValue(repo.Name, rowIndex);
            writers[1].WriteValue(repo.Description, rowIndex);
            writers[2].WriteValue(repo.StargazersCount, rowIndex);
            writers[3].WriteValue(repo.Url, rowIndex);
            writers[4].WriteValue(repo.Owner.Login, rowIndex);
        });

        var data = Connection.Query<(string, string, int, string, string)>("SELECT * FROM github_search('duckdb', 400);").ToList();
    }

    //[Fact]
    //public void RegisterTableFunctionWithTwoParameterTwoColumns()
    //{
    //    var count = 50;

    //    Connection.RegisterTableFunction<short, string>("demo2", (parameters) =>
    //    {
    //        var start = parameters[0].GetValue<short>();
    //        var prefix = parameters[1].GetValue<string>();

    //        return new TableFunction(new List<ColumnInfo>()
    //        {
    //            new ColumnInfo("foo", typeof(int)),
    //            new ColumnInfo("bar", typeof(string)),
    //        }, Enumerable.Range(start, count).Select(index => KeyValuePair.Create(index, prefix + index)));
    //    }, (item, writers, rowIndex) =>
    //    {
    //        var pair = (KeyValuePair<int, string>)item;
    //        writers[0].WriteValue(pair.Key, rowIndex);
    //        writers[1].WriteValue(pair.Value, rowIndex);
    //    });

    //    var data = Connection.Query<(int, string)>($"SELECT * FROM demo2(30::SmallInt, 'DuckDB');").ToList();

    //    data.Select(tuple => tuple.Item1).Should().BeEquivalentTo(Enumerable.Range(30, count));
    //    data.Select(tuple => tuple.Item2).Should().BeEquivalentTo(Enumerable.Range(30, count).Select(i => $"DuckDB{i}"));
    //}

    //[Fact]
    //public void RegisterTableFunctionWithThreeParameters()
    //{
    //    var count = 30;
    //    var startDate = new DateTime(2024, 11, 6);
    //    var minutesParam = 10;
    //    var secondsParam = 2.5;

    //    Connection.RegisterTableFunction<DateTime, long, double>("demo3", (parameters) =>
    //    {
    //        var date = parameters[0].GetValue<DateTime>();
    //        var minutes = parameters[1].GetValue<long>();
    //        var seconds = parameters[2].GetValue<double>();

    //        return new TableFunction(new List<ColumnInfo>()
    //        {
    //            new ColumnInfo("foo", typeof(DateTime)),
    //        }, Enumerable.Range(0, count).Select(i => date.AddDays(i).AddMinutes(minutes).AddSeconds(seconds)));
    //    }, (item, writers, rowIndex) =>
    //    {
    //        writers[0].WriteValue((DateTime)item, rowIndex);
    //    });

    //    var data = Connection.Query<DateTime>($"SELECT * FROM demo3('2024-11-06'::TIMESTAMP, 10, 2.5 );").ToList();

    //    var dateTimes = Enumerable.Range(0, count).Select(i => startDate.AddDays(i).AddMinutes(minutesParam).AddSeconds(secondsParam));
    //    data.Should().BeEquivalentTo(dateTimes);
    //}

    //[Fact]
    //public void RegisterTableFunctionWithFourParameters()
    //{
    //    var guid = Guid.NewGuid();

    //    Connection.RegisterTableFunction<bool, decimal, byte, Guid>("demo4", (parameters) =>
    //    {
    //        var param1 = parameters[0].GetValue<bool>();
    //        var param2 = parameters[1].GetValue<decimal>();
    //        var param3 = parameters[2].GetValue<byte>();
    //        var param4 = parameters[3].GetValue<Guid>();

    //        var enumerable = param4.ToByteArray(param1).Append(param3);

    //        return new TableFunction(new List<ColumnInfo>()
    //        {
    //            new ColumnInfo("foo", typeof(byte)),
    //        }, enumerable);
    //    }, (item, writers, rowIndex) =>
    //    {
    //        writers[0].WriteValue((byte)item, rowIndex);
    //    });

    //    var data = Connection.Query<byte>($"SELECT * FROM demo4(false, 10::DECIMAL(18, 3), 4::UTINYINT, '{guid}'::UUID );").ToList();

    //    var bytes = guid.ToByteArray(false).Append((byte)4);
    //    data.Should().BeEquivalentTo(bytes);
    //}

    //[Fact]
    //public void RegisterTableFunctionWithEmptyResult()
    //{
    //    Connection.RegisterTableFunction<sbyte, ushort, uint, ulong, float>("demo5", (parameters) =>
    //    {
    //        var param1 = parameters[0].GetValue<sbyte>();
    //        var param2 = parameters[1].GetValue<ushort>();
    //        var param3 = parameters[2].GetValue<uint>();
    //        var param4 = parameters[3].GetValue<ulong>();
    //        var param5 = parameters[4].GetValue<float>();

    //        param1.Should().Be(1);
    //        param2.Should().Be(2);
    //        param3.Should().Be(3);
    //        param4.Should().Be(4);
    //        param5.Should().Be(5.6f);

    //        return new TableFunction(new List<ColumnInfo>()
    //        {
    //            new ColumnInfo("foo", typeof(int)),
    //        }, Enumerable.Empty<int>());
    //    }, (item, writers, rowIndex) =>
    //    {
    //        writers[0].WriteValue((int)item, rowIndex);
    //    });

    //    var data = Connection.Query<int>($"SELECT * FROM demo5(1::TINYINT, 2::USMALLINT, 3::UINTEGER, 4::UBIGINT, 5.6);").ToList();

    //    data.Should().BeEquivalentTo(Enumerable.Empty<int>());
    //}
}