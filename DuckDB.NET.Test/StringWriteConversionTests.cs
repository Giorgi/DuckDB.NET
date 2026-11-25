using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Test;

// DuckDB.NET encodes a string to UTF-8 itself, and writes one of up to 12 bytes straight into the vector, in the
// layout of DuckDB's string_t: the length, then 12 bytes with zeros after the string. These tests guard that layout
// and the limits around it. What was written is compared with the bytes DuckDB reads back, and with DuckDB's own
// string of the same characters: an inline string with a wrong byte after its end reads back right and compares wrong.
// If a DuckDB release changes how a short string is stored, these tests fail.
public class StringWriteConversionTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    // One, two, three and four bytes in UTF-8, and a pair of surrogates in the wrong order, which is written as
    // two replacement characters.
    private static readonly string[] Units = ["a", "é", "ა", "\U0001F600", "\uDE00\uD83D"];

    [Fact]
    public void AStringIsStoredAsItsUtf8Bytes()
    {
        var values = new List<string>();

        // Every length around the 12 bytes that are kept inline, and around the 170 characters that are encoded
        // on the stack.
        foreach (var unit in Units)
        {
            foreach (var count in new[] { 0, 1, 2, 3, 4, 5, 6, 7, 11, 12, 13, 14, 85, 169, 170, 171, 172, 600, 5000 })
            {
                values.Add(string.Concat(Enumerable.Repeat(unit, count)));
            }

            // The limit of 12 bytes inside a character.
            for (var ascii = 0; ascii <= 13; ascii++)
            {
                values.Add(new string('x', ascii) + unit);
                values.Add(unit + new string('x', ascii));
            }
        }

        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE stringWriteBytes (value VARCHAR, items VARCHAR[])");

        using (var appender = Connection.CreateAppender("stringWriteBytes"))
        {
            foreach (var value in values)
            {
                appender.CreateRow().AppendValue(value).AppendValue(new List<string> { value, value }).EndRow();
            }
        }

        Command.CommandText = "SELECT hex(value), hex(items[1]), hex(items[2]) FROM stringWriteBytes ORDER BY rowid";
        using var reader = Command.ExecuteReader();

        foreach (var value in values)
        {
            var expected = Convert.ToHexString(Encoding.UTF8.GetBytes(value));

            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be(expected);
            reader.GetString(1).Should().Be(expected);
            reader.GetString(2).Should().Be(expected);
        }

        reader.Read().Should().BeFalse();
    }

    // DuckDB compares and hashes a short string by its 16 bytes, so the bytes after the string have to be zero.
    // Reading the string back does not show them; comparing with a string that DuckDB made does.
    [Fact]
    public void AShortStringEqualsDuckDBsOwn()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE stringWriteEquals (value VARCHAR, length INTEGER)");

        using (var appender = Connection.CreateAppender("stringWriteEquals"))
        {
            for (var length = 0; length <= 14; length++)
            {
                appender.CreateRow().AppendValue(new string('a', length)).AppendValue((int?)length).EndRow();
            }
        }

        Command.CommandText = "SELECT count(*) FROM stringWriteEquals WHERE value = repeat('a', length) AND hash(value) = hash(repeat('a', length))";
        Command.ExecuteScalar().Should().Be(15L);

        Command.CommandText = "SELECT count(*) FROM stringWriteEquals JOIN range(15) r(i) ON value = repeat('a', i::INTEGER)";
        Command.ExecuteScalar().Should().Be(15L);
    }

    // A NUL character is stored like any other; before 2.0, a string ended at its first one.
    private static readonly string[] NulValues = ["\0", "ab\0cd", "abcdefghijk\0", "abcdefghijkl\0mn", new string('n', 200) + "\0tail", "აააა\0ა"];

    [Fact]
    public void AnAppendedStringKeepsItsNulCharacters()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE stringWriteNul (value VARCHAR)");

        using (var appender = Connection.CreateAppender("stringWriteNul"))
        {
            foreach (var value in NulValues)
            {
                appender.CreateRow().AppendValue(value).EndRow();
            }
        }

        Command.CommandText = "SELECT hex(value), value FROM stringWriteNul ORDER BY rowid";
        using var reader = Command.ExecuteReader();

        foreach (var value in NulValues)
        {
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be(Convert.ToHexString(Encoding.UTF8.GetBytes(value)));
            reader.GetString(1).Should().Be(value);
        }
    }

    // A short string with a NUL is kept inside the value. It must compare and hash like the string DuckDB makes of the
    // same characters, which a wrong byte after its end would break while it still reads back right.
    [Fact]
    public void AnAppendedStringWithNulEqualsDuckDBsOwn()
    {
        (string Value, string Sql)[] cases =
        [
            ("\0", "chr(0)"),
            ("ab\0", "'ab' || chr(0)"),
            ("a\0b", "'a' || chr(0) || 'b'"),
            ("abcdefghijk\0", "'abcdefghijk' || chr(0)"),
            ("abcdefghijkl\0", "'abcdefghijkl' || chr(0)"),
        ];

        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE stringWriteNulEquals (value VARCHAR)");

        using (var appender = Connection.CreateAppender("stringWriteNulEquals"))
        {
            foreach (var (value, _) in cases)
            {
                appender.CreateRow().AppendValue(value).EndRow();
            }
        }

        var duckDBStrings = string.Join(", ", cases.Select(c => $"({c.Sql})"));

        Command.CommandText = $"SELECT count(*) FROM stringWriteNulEquals JOIN (VALUES {duckDBStrings}) d(value) " +
                              "ON stringWriteNulEquals.value = d.value AND hash(stringWriteNulEquals.value) = hash(d.value)";
        Command.ExecuteScalar().Should().Be((long)cases.Length);

        // The string without its NUL is another string.
        Command.CommandText = "SELECT count(*) FROM stringWriteNulEquals WHERE value IN ('ab', 'a', 'abcdefghijk', 'abcdefghijkl', '')";
        Command.ExecuteScalar().Should().Be(0L);
    }

    [Fact]
    public void AFunctionResultKeepsItsNulCharacters()
    {
        Connection.RegisterScalarFunction<int, string>("string_with_nul", index => NulValues[index]);

        for (var index = 0; index < NulValues.Length; index++)
        {
            Command.CommandText = $"SELECT hex(string_with_nul({index}))";
            Command.ExecuteScalar().Should().Be(Convert.ToHexString(Encoding.UTF8.GetBytes(NulValues[index])));
        }
    }

    [Fact]
    public void AStringParameterKeepsItsNulCharacters()
    {
        // The empty string too: its bytes are an empty span, which may have no address.
        foreach (var value in NulValues.Append(""))
        {
            var expected = Convert.ToHexString(Encoding.UTF8.GetBytes(value));

            using var command = Connection.CreateCommand();
            command.CommandText = "SELECT hex($1), hex($2), hex($3[1])";
            command.Parameters.Add(new DuckDBParameter(value));
            command.Parameters.Add(new DuckDBParameter(DbType.String, value));
            command.Parameters.Add(new DuckDBParameter(new List<string> { value }));

            using var reader = command.ExecuteReader();
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be(expected);
            reader.GetString(1).Should().Be(expected);
            reader.GetString(2).Should().Be(expected);
        }
    }

    // The writer builds every short string with TryCreateInlined; the tests above check what DuckDB makes of it.
    // This one checks the value itself: the length, the bytes, zeros after them, and which strings are refused.
    // BitConverter writes the length in the machine's byte order, as DuckDB stores it.
    [Theory]
    [InlineData("", true)]
    [InlineData("a", true)]
    [InlineData("twelve chars", true)]
    [InlineData("thirteen char", false)]
    [InlineData("აააა", true)]
    [InlineData("ააააა", false)]
    [InlineData("xxxxxxxxxxxé", false)]
    [InlineData("\U0001F600\U0001F600\U0001F600", true)]
    [InlineData("ab\0", true)]
    [InlineData("\0", true)]
    public unsafe void TryCreateInlinedKeepsAShortStringInsideTheValue(string value, bool inlined)
    {
        // Filled with non-zero bytes first, so that bytes the method leaves alone show: a fresh local is zero anyway.
        DuckDBString result = default;
        new Span<byte>(&result, sizeof(DuckDBString)).Fill(0xFF);

        DuckDBString.TryCreateInlined(value, out result).Should().Be(inlined);

        // The length in 4 bytes, the bytes of the string, then zeros up to 16; a refused string leaves 16 zeros.
        var expected = new byte[16];
        var length = inlined ? Encoding.UTF8.GetBytes(value, expected.AsSpan(4)) : 0;
        BitConverter.TryWriteBytes(expected.AsSpan(0, 4), (uint)length);

        result.Length.Should().Be(length);
        new ReadOnlySpan<byte>(&result, sizeof(DuckDBString)).ToArray().Should().Equal(expected);
    }
}
