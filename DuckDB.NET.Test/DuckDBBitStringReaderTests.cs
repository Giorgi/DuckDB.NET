using System.Collections;
using System.Linq;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBBitStringReaderTests : DuckDBTestBase
{
    public DuckDBBitStringReaderTests(DuckDBDatabaseFixture db) : base(db)
    {
    }

    [Fact]
    public void ReadBitString()
    {
        Command.CommandText = "SELECT bitstring('0101011', 12)";
        var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldType(0).Should().Be(typeof(string));

        var value = reader.GetValue(0);
        value.Should().Be("000000101011");

        value = reader.GetFieldValue<string>(0);
        value.Should().Be("000000101011");
    }

    [Fact]
    public void ReadBitStringAsBitArray()
    {
        Command.CommandText = "SELECT bitstring('0101011', 12)";
        var reader = Command.ExecuteReader();
        reader.Read();

        var expected = new BitArray(new bool[] { false, false, false, false, false, false, true, false, true, false, true, true });

        var value = reader.GetFieldValue<BitArray>(0);

        expected.Xor(value).OfType<bool>().All(b => !b).Should().BeTrue();
    }
}