namespace DuckDB.NET.Test.Parameters;

public class BlobParameterTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void SimpleTest()
    {
        Command.CommandText = "SELECT 'ABCD'::BLOB;";
        Command.ExecuteNonQuery();

        var reader = Command.ExecuteReader();
        reader.Read();

        using (var stream = reader.GetStream(0))
        {
            stream.Length.Should().Be(4);
            stream.CanWrite.Should().Be(false);

            using (var streamReader = new StreamReader(stream, leaveOpen: true))
            {
                var text = streamReader.ReadToEnd();
                text.Should().Be("ABCD");
            }
        }

        using (var streamItem = (Stream)reader.GetValue(0))
        {
            using (var streamReader = new StreamReader(streamItem, leaveOpen: true))
            {
                var text = streamReader.ReadToEnd();
                text.Should().Be("ABCD");
            }
        }

        Command.CommandText = "SELECT 'AB\\x0aCD'::BLOB";
        Command.ExecuteNonQuery();

        reader = Command.ExecuteReader();
        reader.Read();

        using (var stream = reader.GetStream(0))
        {
            stream.Length.Should().Be(5);
            using (var streamReader = new StreamReader(stream, leaveOpen: true))
            {
                var text = streamReader.ReadLine();
                text.Should().Be("AB");

                text = streamReader.ReadLine();
                text.Should().Be("CD");
            }
        }

        reader.GetFieldType(0).Should().Be(typeof(Stream));
    }

    [Fact]
    public void GetFieldValueByteArrayReadsBlob()
    {
        Command.CommandText = "SELECT '\\x01\\x02\\x03\\x04'::BLOB;";

        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();

        reader.GetFieldValue<byte[]>(0).Should().Equal(1, 2, 3, 4);
        reader.GetValue(0).Should().BeOfType<UnmanagedMemoryStream>();
    }

    [Fact]
    public void GetFieldValueByteArrayReadsEmptyBlob()
    {
        Command.CommandText = "SELECT ''::BLOB;";

        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();

        reader.GetFieldValue<byte[]>(0).Should().BeEmpty();
    }

    [Fact]
    public void GetFieldValueByteArrayReadsBlobInAStruct()
    {
        Command.CommandText = "SELECT {'value': '\\x01\\x02\\x03'::BLOB};";

        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();

        reader.GetFieldValue<BlobStruct>(0).Value.Should().Equal(1, 2, 3);
    }

    [Fact]
    public void GetFieldValueByteArrayReadsBlobInAMap()
    {
        Command.CommandText = "SELECT MAP {'value': '\\x04\\x05'::BLOB};";

        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();

        reader.GetFieldValue<Dictionary<string, byte[]>>(0)["value"].Should().Equal(4, 5);
    }

    [Fact]
    public void GetFieldValueByteArrayReadsBlobInAList()
    {
        Command.CommandText = "SELECT ['\\x06\\x07'::BLOB, '\\x08'::BLOB];";

        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();

        reader.GetFieldValue<List<byte[]>>(0).Should().BeEquivalentTo(new[] { new byte[] { 6, 7 }, new byte[] { 8 } });
    }

    [Fact]
    public void SeekTest()
    {
        var blobValue = "ABCDEFGHIJKLMNOPQR";
        Command.CommandText = $"SELECT '{blobValue}'::BLOB;";
        Command.ExecuteNonQuery();

        var reader = Command.ExecuteReader();
        reader.Read();

        using var stream = reader.GetStream(0);
        stream.CanSeek.Should().Be(true);
        using (var streamReader = new StreamReader(stream, leaveOpen: true))
        {
            stream.Seek(2, SeekOrigin.Begin);
            var text = streamReader.ReadToEnd();
            text.Should().Be(blobValue.Substring(2));

            stream.Seek(-4, SeekOrigin.End);
            streamReader.ReadLine().Should().Be(blobValue[^4..]);

            stream.Seek(-4, SeekOrigin.End);
            stream.Seek(2, SeekOrigin.Current);

            streamReader.ReadLine().Should().Be(blobValue[^4..][^4..][2..]);

            stream.Position = 7;
            streamReader.ReadLine().Should().Be(blobValue[7..]);

            stream.Seek(0, SeekOrigin.Begin).Should().Be(0);
            stream.Seek(0, SeekOrigin.End).Should().Be(stream.Length);
            stream.Position = 5;
            stream.Seek(0, SeekOrigin.Current).Should().Be(stream.Position);

            //stream.Invoking(s => s.Seek(stream.Length + 1, SeekOrigin.Current)).Should().Throw<ArgumentOutOfRangeException>();
        }
    }

    [Fact]
    public void BindValueTest()
    {
        Command.CommandText = "CREATE TABLE BlobTests (key INTEGER, value Blob)";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO BlobTests VALUES (9, ?);";

        Command.Parameters.Add(new DuckDBParameter(new byte[] { 65, 66 }));
        Command.ExecuteNonQuery();

        var command = Connection.CreateCommand();
        command.CommandText = "SELECT * from BlobTests;";

        var reader = command.ExecuteReader();
        reader.Read();

        using (var stream = reader.GetStream(1))
        {
            stream.Length.Should().Be(2);
            using (var streamReader = new StreamReader(stream, leaveOpen: true))
            {
                var text = streamReader.ReadLine();
                text.Should().Be("AB");
            }
        }

        reader.GetFieldType(1).Should().Be(typeof(Stream));
    }

    [Fact]
    public void BindParameterWithoutTable()
    {
        var value = Faker.Random.Bytes(Faker.Random.Int(1, 100));

        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));

        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();

        using var stream = (Stream)reader.GetValue(0);
        var resultBytes = new byte[value.Length];

        stream.Read(resultBytes, 0, resultBytes.Length);

        resultBytes.Should().BeEquivalentTo(value);
    }

    private sealed class BlobStruct
    {
        public byte[] Value { get; set; } = [];
    }
}
