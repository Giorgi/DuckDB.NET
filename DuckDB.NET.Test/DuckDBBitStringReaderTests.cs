using System.Collections;

namespace DuckDB.NET.Test;

public class DuckDBBitStringReaderTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
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

    // Each of these reads used to call itself until the stack overflowed, which ends the process.
    [Fact]
    public void ReadBitStringAsUnsupportedTypeThrows()
    {
        Command.CommandText = "SELECT '101'::BIT";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetFieldValue<int>(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFieldValue<int?>(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetInt64(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetBoolean(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetDecimal(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetGuid(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetDateTime(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFieldValue<byte[]>(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFieldValue<List<int>>(0)).Should().Throw<InvalidCastException>();

        reader.GetString(0).Should().Be("101");
        reader.GetFieldValue<object>(0).Should().Be("101");
    }
}