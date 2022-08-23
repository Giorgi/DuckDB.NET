using System;
using System.IO;
using System.Numerics;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class BlobParameterTests
{
    [Fact]
    public void SimpleTest()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = "SELECT 'ABCD'::BLOB;";
        command.ExecuteNonQuery();

        var reader = command.ExecuteReader();
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

        command.CommandText = "SELECT 'AB\\x0aCD'::BLOB";
        command.ExecuteNonQuery();

        reader = command.ExecuteReader();
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
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var command = connection.CreateCommand();
        command.CommandText = "SELECT 'ABCDEFGH'::BLOB;";
        command.ExecuteNonQuery();

        var reader = command.ExecuteReader();
        reader.Read();

        using (var stream = reader.GetStream(0))
        {
            stream.CanSeek.Should().Be(true);
            using (var streamReader = new StreamReader(stream, leaveOpen: true))
            {
                stream.Seek(2, SeekOrigin.Begin);
                var text = streamReader.ReadToEnd();
                text.Should().Be("CDEFGH");

                stream.Seek(-4, SeekOrigin.End);
                streamReader.ReadLine().Should().Be("EFGH");

                stream.Seek(-4, SeekOrigin.End);
                stream.Seek(2, SeekOrigin.Current);

                streamReader.ReadLine().Should().Be("GH");

                stream.Position = 7;
                streamReader.ReadLine().Should().Be("H");

                stream.Seek(0, SeekOrigin.Begin).Should().Be(0);
                stream.Seek(0, SeekOrigin.End).Should().Be(stream.Length);
                stream.Position = 5;
                stream.Seek(0, SeekOrigin.Current).Should().Be(stream.Position);

                stream.Invoking(s => s.Seek(stream.Length+1, SeekOrigin.Current)).Should().Throw<InvalidOperationException>();
            }
        }
    }

    [Fact]
    public void BindValueTest()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        var duckDbCommand = connection.CreateCommand();
        duckDbCommand.CommandText = "CREATE TABLE BlobTests (key INTEGER, value Blob)";
        duckDbCommand.ExecuteNonQuery();

        duckDbCommand.CommandText = "INSERT INTO BlobTests VALUES (9, ?);";

        duckDbCommand.Parameters.Add(new DuckDBParameter(new byte[] { 65, 66 }));
        duckDbCommand.ExecuteNonQuery();

        var command = connection.CreateCommand();
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
}