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

    [Fact]
    public void AStringEndsAtItsFirstNulCharacter()
    {
        string[] values = ["\0", "ab\0cd", "abcdefghijk\0", "abcdefghijkl\0mn", new string('n', 200) + "\0tail", "აააა\0ა"];

        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE stringWriteNul (value VARCHAR)");

        using (var appender = Connection.CreateAppender("stringWriteNul"))
        {
            foreach (var value in values)
            {
                appender.CreateRow().AppendValue(value).EndRow();
            }
        }

        Command.CommandText = "SELECT hex(value) FROM stringWriteNul ORDER BY rowid";
        using var reader = Command.ExecuteReader();

        foreach (var value in values)
        {
            reader.Read().Should().BeTrue();
            reader.GetString(0).Should().Be(Convert.ToHexString(Encoding.UTF8.GetBytes(value[..value.IndexOf('\0')])));
        }
    }
}
