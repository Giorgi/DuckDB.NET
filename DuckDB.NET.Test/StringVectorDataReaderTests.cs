using DuckDB.NET.Data.DataChunk.Reader;

namespace DuckDB.NET.Test;

// GetChars with a null buffer counts the UTF-16 length from the UTF-8 bytes without decoding them. The count must be
// the length of the string that GetString decodes.
public class StringVectorDataReaderTests
{
    [Fact]
    public void CountUtf16CharsCountsEveryScalarValue()
    {
        var all = new StringBuilder();
        for (var scalar = 0; scalar <= 0x10FFFF; scalar++)
        {
            if (scalar is >= 0xD800 and <= 0xDFFF)
            {
                continue;
            }

            var text = char.ConvertFromUtf32(scalar);
            StringVectorDataReader.CountUtf16Chars(Encoding.UTF8.GetBytes(text)).Should().Be(text.Length, "U+{0:X4}", scalar);
            all.Append(text);
        }

        var whole = all.ToString();
        StringVectorDataReader.CountUtf16Chars(Encoding.UTF8.GetBytes(whole)).Should().Be(whole.Length);
    }

    [Fact]
    public void CountUtf16CharsCountsMixedTextOfEveryLengthAtEveryAlignment()
    {
        string[] pieces = ["a", "\u007f", "é", "߿", "ࠀ", "ქ", "￿", "\U00010000", "\U0001F986", "\U0010FFFF"];
        var random = new Random(4);

        for (var round = 0; round < 3000; round++)
        {
            var text = new StringBuilder();
            var count = random.Next(0, 120);
            for (var i = 0; i < count; i++)
            {
                text.Append(pieces[random.Next(pieces.Length)]);
            }

            var bytes = Encoding.UTF8.GetBytes(text.ToString());

            // The vector loop reads 16 bytes at a time, so the same bytes are counted at every offset of a buffer.
            for (var shift = 0; shift < 17; shift++)
            {
                var shifted = new byte[bytes.Length + shift];
                bytes.CopyTo(shifted, shift);
                StringVectorDataReader.CountUtf16Chars(shifted.AsSpan(shift)).Should().Be(text.Length);
            }
        }
    }
}
