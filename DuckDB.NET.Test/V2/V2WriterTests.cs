using DuckDB.NET.Data.DataChunk.Reader;
using DuckDB.NET.Data.DataChunk.Writer;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Test.V2;

// The writers on V2 vectors, before the appender and the functions use them: each test writes one column of a data
// chunk created through V2 and reads it back with the V2 readers. A round trip cannot catch a constant that is wrong
// in both directions; the writer tests that compare with DuckDB do that once the appender is on V2.
[Trait("V2", "Ready")]
public unsafe class V2WriterTests
{
    [Fact]
    public void IntegersAndNullsAreWritten()
    {
        RoundTrip("INTEGER", writer =>
        {
            writer.WriteValue(1, 0);
            writer.WriteNull(1);
            writer.WriteValue((int?)-3, 2);
        }, 3, reader =>
        {
            reader.GetValue<int>(0).Should().Be(1);
            reader.IsValid(1).Should().BeFalse();
            reader.GetValue<int>(2).Should().Be(-3);
        });
    }

    [Fact]
    public void ShortAndLongStringsAreWritten()
    {
        var longText = new string('x', 5000) + "ü";

        RoundTrip("VARCHAR", writer =>
        {
            writer.WriteValue("", 0);
            writer.WriteValue("twelve bytes", 1);
            writer.WriteValue("thirteen byte", 2);
            writer.WriteValue(longText, 3);
            writer.WriteNull(4);
        }, 5, reader =>
        {
            reader.GetValue<string>(0).Should().Be("");
            reader.GetValue<string>(1).Should().Be("twelve bytes");
            reader.GetValue<string>(2).Should().Be("thirteen byte");
            reader.GetValue<string>(3).Should().Be(longText);
            reader.IsValid(4).Should().BeFalse();
        });
    }

    [Fact]
    public void ManyLongStringsAreWritten()
    {
        // Enough out-of-line bytes to need more than one block of the vector's arena.
        const int rows = 2048;
        RoundTrip("VARCHAR", writer =>
        {
            for (var i = 0; i < rows; i++)
            {
                writer.WriteValue($"row {i} " + new string('y', 1000), (ulong)i);
            }
        }, rows, reader =>
        {
            for (var i = 0; i < rows; i++)
            {
                reader.GetValue<string>((ulong)i).Should().Be($"row {i} " + new string('y', 1000));
            }
        });
    }

    [Fact]
    public void BlobsAreWritten()
    {
        byte[] small = [1, 2, 3];
        var large = Enumerable.Range(0, 100).Select(i => (byte)i).ToArray();

        RoundTrip("BLOB", writer =>
        {
            writer.WriteValue(small, 0);
            writer.WriteValue(large, 1);
        }, 2, reader =>
        {
            ReadBlob(reader, 0).Should().Equal(small);
            ReadBlob(reader, 1).Should().Equal(large);
        });
    }

    [Theory]
    [InlineData("DECIMAL(4,1)")]
    [InlineData("DECIMAL(9,3)")]
    [InlineData("DECIMAL(18,3)")]
    [InlineData("DECIMAL(38,10)")]
    public void DecimalsAreWrittenInTheStorageOfTheirWidth(string type)
    {
        RoundTrip(type, writer =>
        {
            writer.WriteValue(12.5m, 0);
            writer.WriteValue(-0.5m, 1);
        }, 2, reader =>
        {
            reader.GetValue<decimal>(0).Should().Be(12.5m);
            reader.GetValue<decimal>(1).Should().Be(-0.5m);
        });
    }

    [Fact]
    public void ADecimalTooWideForTheColumnIsRefused()
    {
        var act = () => RoundTrip("DECIMAL(4,1)", writer => writer.WriteValue(1000m, 0), 1, _ => { });
        act.Should().Throw<Exception>();
    }

    [Fact]
    public void EnumsAreWrittenByName()
    {
        RoundTrip("ENUM('small', 'medium', 'large')", writer =>
        {
            writer.WriteValue("large", 0);
            writer.WriteValue("small", 1);
            writer.WriteNull(2);
        }, 3, reader =>
        {
            reader.GetValue<string>(0).Should().Be("large");
            reader.GetValue<string>(1).Should().Be("small");
            reader.IsValid(2).Should().BeFalse();
        });
    }

    [Fact]
    public void AnEnumWithMoreThan255EntriesIsWrittenAsUInt16()
    {
        var names = Enumerable.Range(0, 300).Select(i => $"e{i}").ToList();
        RoundTrip($"ENUM({string.Join(", ", names.Select(n => $"'{n}'"))})", writer =>
        {
            writer.WriteValue("e299", 0);
            writer.WriteValue("e3", 1);
        }, 2, reader =>
        {
            reader.GetValue<string>(0).Should().Be("e299");
            reader.GetValue<string>(1).Should().Be("e3");
        });
    }

    [Fact]
    public void ListsAreWritten()
    {
        RoundTrip("INTEGER[]", writer =>
        {
            writer.WriteValue(new List<int?> { 1, null, 3 }, 0);
            writer.WriteNull(1);
            writer.WriteValue(new List<int?>(), 2);
            writer.WriteValue(new List<int?> { 4 }, 3);
        }, 4, reader =>
        {
            reader.GetValue<List<int?>>(0).Should().Equal(1, null, 3);
            reader.IsValid(1).Should().BeFalse();
            reader.GetValue<List<int?>>(2).Should().BeEmpty();
            reader.GetValue<List<int?>>(3).Should().Equal(4);
        });
    }

    [Fact]
    public void LongListsGrowTheItemVector()
    {
        // More items than one vector holds, so the items' vector is resized and its writer fetches its pointers again.
        const int rows = 100;
        RoundTrip("VARCHAR[]", writer =>
        {
            for (var i = 0; i < rows; i++)
            {
                writer.WriteValue(Enumerable.Range(0, 100).Select(j => $"item {i}-{j} with some length").ToList(), (ulong)i);
            }
        }, rows, reader =>
        {
            for (var i = 0; i < rows; i++)
            {
                reader.GetValue<List<string>>((ulong)i).Should().Equal(Enumerable.Range(0, 100).Select(j => $"item {i}-{j} with some length"));
            }
        });
    }

    [Fact]
    public void ArraysAreWritten()
    {
        RoundTrip("VARCHAR[2]", writer =>
        {
            writer.WriteValue(new List<string> { "a", "a long string of more than twelve bytes" }, 0);
            writer.WriteNull(1);
            writer.WriteValue(new List<string> { null, "c" }, 2);
        }, 3, reader =>
        {
            reader.GetValue<List<string>>(0).Should().Equal("a", "a long string of more than twelve bytes");
            reader.IsValid(1).Should().BeFalse();
            reader.GetValue<List<string>>(2).Should().Equal(null, "c");
        });
    }

    [Fact]
    public void AnArrayOfTheWrongLengthIsRefused()
    {
        var act = () => RoundTrip("INTEGER[2]", writer => writer.WriteValue(new List<int> { 1, 2, 3 }, 0), 1, _ => { });
        act.Should().Throw<Exception>();
    }

    private static byte[] ReadBlob(VectorDataReaderBase reader, ulong row)
    {
        using var stream = reader.GetValue<Stream>(row);
        using var copy = new MemoryStream();
        stream.CopyTo(copy);
        return copy.ToArray();
    }

    private static void RoundTrip(string typeText, Action<VectorDataWriterBase> write, ulong rows, Action<VectorDataReaderBase> check)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var lease = connection.V2Connection.Acquire();

        _duckdb_v2_error_info* error = null;
        _duckdb_v2_factory* factory;
        NativeMethodsV2.duckdb_v2_connection_get_factory(lease.Pointer, &factory, &error).ThrowIfFailed(ref error);

        var text = System.Text.Encoding.UTF8.GetBytes(typeText);
        _duckdb_v2_logical_type* type;
        fixed (byte* pointer = text)
        {
            NativeMethodsV2.duckdb_v2_factory_create_type_from_text(factory, (sbyte*)pointer, (ulong)text.Length, &type, &error).ThrowIfFailed(ref error);
        }

        _duckdb_v2_data_chunk* chunk = null;

        try
        {
            NativeMethodsV2.duckdb_v2_data_chunk_create(factory, &type, 1, &chunk, &error).ThrowIfFailed(ref error);

            _duckdb_v2_vector* vector;
            NativeMethodsV2.duckdb_v2_data_chunk_get_vector(chunk, 0, &vector, &error).ThrowIfFailed(ref error);

            using (var writer = VectorDataWriterFactory.CreateWriter(vector, type))
            {
                write(writer);
            }

            NativeMethodsV2.duckdb_v2_vector_set_size(vector, rows, &error).ThrowIfFailed(ref error);

            using var reader = VectorDataReaderFactory.CreateReader(type, "c");
            reader.Reset(vector);
            check(reader);
        }
        finally
        {
            NativeMethodsV2.duckdb_v2_data_chunk_destroy(&chunk);
            NativeMethodsV2.duckdb_v2_logical_type_destroy(&type);
        }
    }
}
