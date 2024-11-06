using System;
using Dapper;
using DuckDB.NET.Data;
using FluentAssertions;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Xunit;

namespace DuckDB.NET.Test;

[Experimental("DuckDBNET001")]
public class TableFunctionTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void RegisterTableFunctionWithOneParameter()
    {
        Connection.RegisterTableFunction<int>("demo", (parameters) =>
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
    public void RegisterTableFunctionWithTwoParameterTwoColumns()
    {
        var count = 50;

        Connection.RegisterTableFunction<short, string>("demo2", (parameters) =>
        {
            var start = parameters[0].GetValue<short>();
            var prefix = parameters[1].GetValue<string>();

            return new TableFunction(new List<ColumnInfo>()
            {
                new ColumnInfo("foo", typeof(int)),
                new ColumnInfo("bar", typeof(string)),
            }, Enumerable.Range(start, count).Select(index => KeyValuePair.Create(index, prefix + index)));
        }, (item, writers, rowIndex) =>
        {
            var pair = (KeyValuePair<int, string>)item;
            writers[0].WriteValue(pair.Key, rowIndex);
            writers[1].WriteValue(pair.Value, rowIndex);
        });

        var data = Connection.Query<(int, string)>($"SELECT * FROM demo2(30::SmallInt, 'DuckDB');").ToList();

        data.Select(tuple => tuple.Item1).Should().BeEquivalentTo(Enumerable.Range(30, count));
        data.Select(tuple => tuple.Item2).Should().BeEquivalentTo(Enumerable.Range(30, count).Select(i => $"DuckDB{i}"));
    }

    [Fact]
    public void RegisterTableFunctionWithThreeParameters()
    {
        var count = 30;
        var startDate = new DateTime(2024, 11, 6);
        var minutesParam= 10;
        var secondsParam = 2.5;

        Connection.RegisterTableFunction<DateTime, long, double>("demo3", (parameters) =>
        {
            var date = parameters[0].GetValue<DateTime>();
            var minutes = parameters[1].GetValue<long>();
            var seconds = parameters[2].GetValue<double>();

            return new TableFunction(new List<ColumnInfo>()
            {
                new ColumnInfo("foo", typeof(DateTime)),
            }, Enumerable.Range(0, count).Select(i => date.AddDays(i).AddMinutes(minutes).AddSeconds(seconds)));
        }, (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((DateTime)item, rowIndex);
        });

        var data = Connection.Query<DateTime>($"SELECT * FROM demo3('2024-11-06'::TIMESTAMP, 10, 2.5 );").ToList();

        var dateTimes = Enumerable.Range(0, count).Select(i => startDate.AddDays(i).AddMinutes(minutesParam).AddSeconds(secondsParam));
        data.Select(dateTime => dateTime).Should().BeEquivalentTo(dateTimes);
    }
}