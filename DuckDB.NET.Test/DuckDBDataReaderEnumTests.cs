using System;
using System.Collections.Generic;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBDataReaderEnumTests : DuckDBTestBase
{
    public DuckDBDataReaderEnumTests(DuckDBDatabaseFixture db) : base(db)
    {
        Command.CommandText = "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');";
        Command.ExecuteNonQuery();

        Command.CommandText = "CREATE TABLE person (name text, current_mood mood);";
        Command.ExecuteNonQuery();

        Command.CommandText = "INSERT INTO person VALUES ('Pedro', 'happy'), ('Mark', NULL), ('Pagliacci', 'sad'), ('Mr. Mackey', 'ok'), ('გიორგი', 'happy');";
        Command.ExecuteNonQuery();
    }

    [Fact]
    public void SelectEnumValues()
    {
        Command.CommandText = "Select * from person order by name desc";
        using var reader = Command.ExecuteReader();
        reader.Read();
        reader.GetFieldValue<Mood>(1).Should().Be(Mood.Happy);

        reader.Read();
        reader.GetFieldValue<Mood>(1).Should().Be(Mood.Happy);

        reader.Read();
        reader.GetFieldValue<Mood>(1).Should().Be(Mood.Sad);

        reader.Read();
        reader.GetFieldValue<Mood>(1).Should().Be(Mood.Ok);

        reader.Read();
        reader.GetFieldValue<Mood?>(1).Should().BeNull();

        reader.Invoking(dataReader => dataReader.GetFieldValue<Mood>(1)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void SelectEnumList()
    {
        Command.CommandText = "Select ['happy'::mood, 'ok'::mood]";
        using var reader = Command.ExecuteReader();
        reader.Read();
        var list = reader.GetFieldValue<List<Mood>>(0);

        list.Should().BeEquivalentTo(new List<Mood> { Mood.Happy, Mood.Ok });
    }

    [Fact]
    public void SelectEnumValuesAsNullable()
    {
        Command.CommandText = "Select * from person order by name desc";
        using var reader = Command.ExecuteReader();
        reader.Read();
        reader.GetFieldValue<Mood?>(1).Should().Be(Mood.Happy);

        reader.Read();
        reader.GetFieldValue<Mood?>(1).Should().Be(Mood.Happy);

        reader.Read();
        reader.GetFieldValue<Mood?>(1).Should().Be(Mood.Sad);

        reader.Read();
        reader.GetFieldValue<Mood?>(1).Should().Be(Mood.Ok);

        reader.Read();
        reader.GetFieldValue<Mood?>(1).Should().Be(null);
    }

    [Fact]
    public void SelectEnumValuesAsString()
    {
        Command.CommandText = "Select * from person order by name desc";
        using var reader = Command.ExecuteReader();
        reader.Read();
        reader.GetFieldValue<string>(1).Should().BeEquivalentTo(Mood.Happy.ToString());

        reader.Read();
        reader.GetFieldValue<string>(1).Should().BeEquivalentTo(Mood.Happy.ToString());

        reader.Read();
        reader.GetValue(1).ToString().Should().BeEquivalentTo(Mood.Sad.ToString());
    }

    public override void Dispose()
    {
        Command.CommandText = "Drop table person";
        Command.ExecuteNonQuery();

        Command.CommandText = "Drop type mood";
        Command.ExecuteNonQuery();

        base.Dispose();
    }

    public enum Mood
    {
        Sad, Ok, Happy
    }
}