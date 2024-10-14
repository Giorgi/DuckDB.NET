using System.Collections.Generic;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class ListParameterTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void CanBindList()
    {
        Command.CommandText = "Select ?";
        var parameter = new DuckDBParameter(new List<int> {1,2,3});
        
        Command.Parameters.Add(parameter);

        using var reader = Command.ExecuteReader();
        reader.Read();
        reader.GetFieldValue<List<int>>(0).Should().BeEquivalentTo(new List<int>{1,2,3});
    }
}