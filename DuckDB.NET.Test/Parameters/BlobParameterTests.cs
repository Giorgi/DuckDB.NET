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

        reader.GetValue(0).Should().BeOfType<byte[]>().Which.Should().Equal("ABCD"u8.ToArray());

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

        reader.GetFieldType(0).Should().Be(typeof(byte[]));
    }

    [Fact]
    public void GetFieldValueByteArrayReadsBlob()
    {
        Command.CommandText = "SELECT '\\x01\\x02\\x03\\x04'::BLOB;";

        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();

        reader.GetFieldValue<byte[]>(0).Should().Equal(1, 2, 3, 4);
        reader.GetValue(0).Should().BeOfType<byte[]>().Which.Should().Equal(1, 2, 3, 4);
        reader.GetProviderSpecificValue(0).Should().BeOfType<byte[]>().Which.Should().Equal(1, 2, 3, 4);
        reader.GetFieldValue<Stream>(0).Should().BeOfType<UnmanagedMemoryStream>();
    }

    // A BLOB used to come back from GetValue as a stream over the chunk's memory, which ExecuteScalar returned after
    // closing its reader: once later results reused that memory, the stream read their bytes.
    [Fact]
    public void ExecuteScalarBlobKeepsItsBytesAfterTheReaderIsClosed()
    {
        Command.CommandText = "SELECT repeat('x', 5000)::BLOB";
        var value = Command.ExecuteScalar();

        for (var i = 0; i < 5; i++)
        {
            using var command = Connection.CreateCommand();
            command.CommandText = "SELECT repeat('y', 5000)::BLOB FROM range(2000)";
            using var reader = command.ExecuteReader();
            while (reader.Read()) { }
        }

        value.Should().BeOfType<byte[]>().Which.Should().Equal(Enumerable.Repeat((byte)'x', 5000));
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

        reader.GetFieldType(1).Should().Be(typeof(byte[]));
    }

    [Fact]
    public void BindParameterWithoutTable()
    {
        var value = Faker.Random.Bytes(Faker.Random.Int(1, 100));

        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));

        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();

        reader.GetValue(0).Should().BeOfType<byte[]>().Which.Should().Equal(value);
    }

    [Fact]
    public void GetValueOfABlobListIsAListOfByteArrays()
    {
        Command.CommandText = "SELECT ['\\x01'::BLOB, NULL, '\\x02\\x03'::BLOB], ['\\x04'::BLOB, '\\x05'::BLOB]::BLOB[2];";
        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();

        reader.GetValue(0).Should().BeOfType<List<byte[]>>()
              .Which.Should().BeEquivalentTo(new[] { new byte[] { 1 }, null, new byte[] { 2, 3 } }, options => options.WithStrictOrdering());
        reader.GetValue(1).Should().BeOfType<List<byte[]>>()
              .Which.Should().BeEquivalentTo(new[] { new byte[] { 4 }, new byte[] { 5 } }, options => options.WithStrictOrdering());
    }

    [Fact]
    public void SchemaTableAndDataTableHaveByteArrayForABlobColumn()
    {
        Command.CommandText = "SELECT b FROM (VALUES ('\\x01\\x02'::BLOB, 1), (NULL, 2)) t(b, i) ORDER BY i;";

        using (var reader = Command.ExecuteReader())
        {
            reader.GetSchemaTable().Rows[0][System.Data.Common.SchemaTableColumn.DataType].Should().Be(typeof(byte[]));
        }

        using (var reader = Command.ExecuteReader())
        {
            var table = new DataTable();
            table.Load(reader);

            table.Columns["b"].DataType.Should().Be(typeof(byte[]));
            table.Rows[0]["b"].Should().BeOfType<byte[]>().Which.Should().Equal(1, 2);
            table.Rows[1]["b"].Should().Be(DBNull.Value);
        }
    }

    [Fact]
    public void DapperReadsABlobAsByteArray()
    {
        Connection.Query<byte[]>("SELECT '\\x01\\x02'::BLOB").Single().Should().Equal(1, 2);
        Connection.Query<BlobStruct>("SELECT '\\x03\\x04'::BLOB AS Value").Single().Value.Should().Equal(3, 4);
    }

    private sealed class BlobStruct
    {
        public byte[] Value { get; set; } = [];
    }
}
