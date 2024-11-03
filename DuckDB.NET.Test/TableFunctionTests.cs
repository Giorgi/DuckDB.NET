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
            var value = parameters.ElementAt(0).GetValue<int>();

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
    public void RegisterTableFunctionWithOneParameterTwoColumns()
    {
        var count = 3000;

        Connection.RegisterTableFunction<int>("demo2", (parameters) =>
        {
            var value = parameters.ElementAt(0).GetValue<int>();

            return new TableFunction(new List<ColumnInfo>()
            {
                new ColumnInfo("foo", typeof(int)),
                new ColumnInfo("bar", typeof(string)),
            }, Enumerable.Range(0, value));
        }, (item, writers, rowIndex) =>
        {
            writers[0].WriteValue((int)item, rowIndex);
            writers[1].WriteValue($"string{item}", rowIndex);
        });

        var data = Connection.Query<(int, string)>($"SELECT * FROM demo2({count});").ToList();

        data.Select(tuple => tuple.Item1).Should().BeEquivalentTo(Enumerable.Range(0, count));
        data.Select(tuple => tuple.Item2).Should().BeEquivalentTo(Enumerable.Range(0, count).Select(i => $"string{i}"));
    }
}