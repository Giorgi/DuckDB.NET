using DuckDB.NET.Test.Helpers;

namespace DuckDB.NET.Test.V2;

// Stage 2, step 3 of docs/V2-MIGRATION.md: LIST, ARRAY, STRUCT and MAP through the C API V2. Their child types come
// from the type's parameters and their child vectors from vector_get_child, after the parent is flattened.
[Trait("V2", "Ready")]
public class V2NestedTypeTests
{
    private static DuckDBConnection Open()
    {
        var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        return connection;
    }

    private static DuckDBDataReader Query(DuckDBConnection connection, string sql)
    {
        var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteReader();
    }

    public class Point
    {
        public int X { get; set; }
        public string Label { get; set; }
        public Dictionary<string, int> Tags { get; set; }
    }

    [Fact]
    public void AListOfStructsOfMapsIsRead()
    {
        using var connection = Open();
        using var reader = Query(connection, "SELECT [{'X': 1, 'Label': 'a', 'Tags': MAP {'k': 10}}, NULL, {'X': 2, 'Label': NULL, 'Tags': MAP {}}] AS points");
        reader.Read().Should().BeTrue();

        var points = reader.GetFieldValue<List<Point>>(0);

        points.Should().HaveCount(3);
        points[0].X.Should().Be(1);
        points[0].Label.Should().Be("a");
        points[0].Tags.Should().Equal(new Dictionary<string, int> { ["k"] = 10 });
        points[1].Should().BeNull();
        points[2].Label.Should().BeNull();
        points[2].Tags.Should().BeEmpty();
    }

    [Fact]
    public void ArraysOfListsAndMapsOfListsAreRead()
    {
        using var connection = Open();
        using var reader = Query(connection, "SELECT [[1, 2], [], NULL]::INTEGER[][3] AS a, MAP {'x': [1, NULL], 'y': NULL} AS m");
        reader.Read().Should().BeTrue();

        reader.GetFieldValue<List<List<int?>>>(0).Should().BeEquivalentTo(new List<List<int?>> { new() { 1, 2 }, new(), null },
                                                                         options => options.WithStrictOrdering());
        var map = reader.GetFieldValue<Dictionary<string, List<int?>>>(1);
        map["x"].Should().Equal(1, null);
        map["y"].Should().BeNull();
    }

    [Fact]
    public void NestedValuesAreReadAcrossChunks()
    {
        using var connection = Open();
        using var reader = Query(connection, "SELECT i, [i, i + 1] AS l, {'a': i, 'b': [i::VARCHAR]} AS s, " +
                                             "CASE WHEN i % 7 = 0 THEN NULL ELSE MAP {i: 'v' || i} END AS m " +
                                             "FROM range(5000) t(i) ORDER BY i");
        var rows = 0;

        while (reader.Read())
        {
            var i = reader.GetInt64(0);
            reader.GetFieldValue<List<long>>(1).Should().Equal(i, i + 1);

            var s = reader.GetFieldValue<Dictionary<string, object>>(2);
            s["a"].Should().Be(i);
            ((List<string>)s["b"]).Should().Equal(i.ToString());

            if (i % 7 == 0)
            {
                reader.IsDBNull(3).Should().BeTrue();
            }
            else
            {
                reader.GetFieldValue<Dictionary<long, string>>(3)[i].Should().Be($"v{i}");
            }

            rows++;
        }

        rows.Should().Be(5000);
    }

    [Fact]
    public void TheSameNestedValueInEveryRowIsRead()
    {
        // A value that is the same in every row is where DuckDB can hand out a constant vector; flattening it first
        // gives every row its own entry.
        using var connection = Open();
        using var reader = Query(connection, "SELECT [1, 2, 3] AS l, {'a': 'x'} AS s, MAP {'k': 1} AS m FROM range(3000)");
        var rows = 0;

        while (reader.Read())
        {
            reader.GetFieldValue<List<int>>(0).Should().Equal(1, 2, 3);
            reader.GetFieldValue<Dictionary<string, object>>(1)["a"].Should().Be("x");
            reader.GetFieldValue<Dictionary<string, int>>(2)["k"].Should().Be(1);
            rows++;
        }

        rows.Should().Be(3000);
    }

    [Fact]
    public void NestedColumnTypesAreReportedWithoutRows()
    {
        using var connection = Open();
        using var reader = Query(connection, "SELECT [1]::INTEGER[] AS l, {'a': 'x'} AS s, MAP {'k': 1.5} AS m WHERE false");

        reader.Read().Should().BeFalse();
        reader.GetFieldType(0).Should().Be(typeof(List<int>));
        reader.GetFieldType(2).Should().Be(typeof(Dictionary<string, decimal>));
        reader.GetDataTypeName(1).Should().Be(nameof(DuckDBType.Struct));
    }

    [Fact]
    public void AFileDatabaseIsNamedAfterItsFile()
    {
        // V1's duckdb_open named the default database after the file; V2 attaches it, with the same name.
        using var file = DisposableFile.GenerateInTemp("db");
        using var connection = new DuckDBConnection(file.ConnectionString);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT current_database()";

        command.ExecuteScalar().Should().Be(Path.GetFileNameWithoutExtension(file.FileName));
    }

    [Fact]
    public void AnInMemoryDatabaseIsNamedMemory()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT current_database()";

        command.ExecuteScalar().Should().Be("memory");
    }
}
