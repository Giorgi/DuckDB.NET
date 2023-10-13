using System.Collections.Generic;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBDataReaderNestedListTests : DuckDBTestBase
{
    public DuckDBDataReaderNestedListTests(DuckDBDatabaseFixture db) : base(db)
    {
        Command.CommandText = "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');";
        Command.ExecuteNonQuery();
    }

    [Fact]
    public void ReadNestedListOfIntegers()
    {
        Command.CommandText = "SELECT [[1,2,3], [4,5], [6]];";
        using var reader = Command.ExecuteReader();
        reader.Read();
        var list = reader.GetFieldValue<List<List<int>>>(0);
        list.Should().BeEquivalentTo(new List<List<int>> { new() { 1, 2, 3 }, new() { 4, 5 }, new() { 6 } });
    }

    [Fact]
    public void ReadNestedListOfIntegersAsValue()
    {
        Command.CommandText = "SELECT [[1,2,3], [4,5], [6]];";
        using var reader = Command.ExecuteReader();
        reader.Read();
        var list = reader.GetValue(0);
        list.Should().BeEquivalentTo(new List<List<int>> { new() { 1, 2, 3 }, new() { 4, 5 }, new() { 6 } });
    }

    [Fact]
    public void ReadNestedListOfIntegersWithNulls()
    {
        Command.CommandText = "SELECT [[1,2,3], [4, NULL, 5], [6], NULL];";
        using var reader = Command.ExecuteReader();
        reader.Read();
        var list = reader.GetFieldValue<List<List<int?>>>(0);
        list.Should().BeEquivalentTo(new List<List<int?>> { new() { 1, 2, 3 }, new() { 4, null, 5 }, new() { 6 }, null });
    }

    [Fact]
    public void ReadNestedListOfEnums()
    {
        Command.CommandText = "SELECT [['happy'::mood, 'ok'::mood], NULL, ['ok'::mood, NULL::mood, 'happy'::mood]] order by 1";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var list = reader.GetFieldValue<List<List<DuckDBDataReaderEnumTests.Mood?>>>(0);
        list.Should().BeEquivalentTo(new List<List<DuckDBDataReaderEnumTests.Mood?>>
        {
            new() { DuckDBDataReaderEnumTests.Mood.Happy , DuckDBDataReaderEnumTests.Mood.Ok},
            null,
            new() { DuckDBDataReaderEnumTests.Mood.Ok , null, DuckDBDataReaderEnumTests.Mood.Happy}
        });
    }

    [Fact]
    public void ReadNestedListOfEnumsAsValue()
    {
        Command.CommandText = "SELECT [['happy'::mood, 'ok'::mood], NULL, ['ok'::mood, NULL::mood, 'happy'::mood]] order by 1";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var list = reader.GetValue(0);
        list.Should().BeEquivalentTo(new List<List<string>>
        {
            new() { "happy" , "ok"},
            null,
            new() { "ok" , null, "happy"}
        });
    }

    public override void Dispose()
    {
        Command.CommandText = "Drop type mood";
        Command.ExecuteNonQuery();

        base.Dispose();
    }
}