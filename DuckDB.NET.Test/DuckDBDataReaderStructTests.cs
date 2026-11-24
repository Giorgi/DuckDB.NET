namespace DuckDB.NET.Test;

public class DuckDBDataReaderStructTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void ReadBasicStruct()
    {
        Command.CommandText = "SELECT {'x': 1, 'y': 2, 'z': 'test'};";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var value = reader.GetFieldValue<Struct1>(0);
        value.Should().BeEquivalentTo(new Struct1
        {
            X = 1,
            Y = 2,
            Z = "test"
        });
    }

    [Fact]
    public void ReadBasicStructWithGetValue()
    {
        Command.CommandText = "SELECT {'x': 1, 'y': 2, 'z': 'test', 'xy': null};";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var value = reader.GetValue(0);
        value.Should().BeEquivalentTo(new Dictionary<string, object> { { "x", 1 }, { "y", 2 }, { "z", "test" }, {"xy", null} });
    }

    [Fact]
    public void ReadBasicStructList()
    {
        Command.CommandText = "SELECT [{'x': 1, 'y': 2, 'z': 'test'}, {'x': 4, 'y': 3, 'z': 'tset'}, NULL];";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var value = reader.GetFieldValue<List<Struct1>>(0);
        value.Should().BeEquivalentTo(new List<Struct1>
        {
            new() { X = 1, Y = 2, Z = "test" },
            new() { X = 4, Y = 3, Z = "tset" },
            null
        });
    }

    [Fact]
    public void ReadBasicStructListWithGetValue()
    {
        Command.CommandText = "SELECT [{'x': 1, 'y': 2, 'z': 'test'}, {'x': 4, 'y': 3, 'z': 'tset'}, NULL];";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var value = reader.GetValue(0);
        value.Should().BeEquivalentTo(new List<Dictionary<string, object>>
        {
            new() { { "x", 1 }, { "y", 2 }, { "z", "test" } },
            new() { { "x", 4 }, { "y", 3 }, { "z", "tset" } },
            null
        });
    }

    [Fact]
    public void ReadStructWithNull()
    {
        Command.CommandText = "SELECT {'yes': 'duck', 'maybe': 'goose', 'huh': NULL, 'no': 'heron', 'type': 0} Union All " +
                              "SELECT {'yes': 'duck', 'maybe': 'goose', 'huh': 'bird', 'no': 'heron', 'type':1};";
        using var reader = Command.ExecuteReader();

        reader.Read();
        var value = reader.GetFieldValue<Struct2>(0);

        value.Should().BeEquivalentTo(new Struct2
        {
            Huh = null,
            Maybe = "goose",
            No = "heron",
            Yes = "duck",
            Type = 0
        });

        reader.Read();
        value = reader.GetFieldValue<Struct2>(0);

        value.Should().BeEquivalentTo(new Struct2
        {
            Huh = "bird",
            Maybe = "goose",
            No = "heron",
            Yes = "duck",
            Type = 1
        });
    }
    
    [Fact]
    public void ReadStructWithNestedStruct()
    {
        Command.CommandText = "SELECT {'birds': {'yes': 'duck', 'maybe': 'goose', 'huh': NULL, 'no': 'heron', 'type': 0}, " +
                              "'aliens': NULL, " +
                              "'amphibians': {'yes':'frog', 'maybe': 'salamander', 'huh': 'dragon', 'no':'toad', 'type':1} };";
        using var reader = Command.ExecuteReader();

        reader.Read();
        var value = reader.GetFieldValue<Struct3>(0);
        
        value.Aliens.Should().BeNull();

        value.Birds.Should().BeEquivalentTo(new Struct2
        {
            Huh = null,
            Maybe = "goose",
            No = "heron",
            Yes = "duck",
            Type = 0
        });

        value.Amphibians.Should().BeEquivalentTo(new Struct2
        {
            Huh = "dragon",
            Maybe = "salamander",
            No = "toad",
            Yes = "frog",
            Type = 1
        });
    }

    [Theory]
    //[InlineData("SELECT {'b': null};", $"Property '{nameof(Struct4.A)}' not found in struct")]
    [InlineData("SELECT {'b': null, 'a': 4};", $"Property '{nameof(Struct4.B)}' is not nullable but struct contains null value")]
    public void ReadStructWithMissingDataThrowsException(string query, string error)
    {
        Command.CommandText = query;
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetFieldValue<Struct4>(0)).Should().Throw<InvalidCastException>().WithMessage(error);
    }

    class Struct1
    {
        public int X { get; set; }
        public int Y { get; set; }
        public string Z { get; set; }
        public int XY { get; }
    }

    class Struct2
    {
        public string Yes { get; set; }
        public string Maybe { get; set; }
        public string Huh { get; set; }
        public string No { get; set; }
        public int Type { get; set; }
    }

    class Struct3
    {
        public Struct2 Birds { get; set; }
        public Struct2 Aliens { get; set; }
        public Struct2 Amphibians { get; set; }
    }

    class Struct4
    {
        public int A { get; set; }
        public int B { get; set; }
    }
}