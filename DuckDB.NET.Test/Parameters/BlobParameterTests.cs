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

    [Fact(Skip = "Failing on mac")]
    public void BindParameterWithoutTable()
    {
        var value = Faker.Random.Bytes(Faker.Random.Int(1, 100));
        
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));
        
        var result = Command.ExecuteScalar();
        
        using var stream = (Stream)result;
        var resultBytes = new byte[value.Length];
        
        stream.Read(resultBytes, 0, resultBytes.Length);

        resultBytes.Should().BeEquivalentTo(value);
    }
}