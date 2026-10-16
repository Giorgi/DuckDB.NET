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

    // The reader used to hand the stored bytes to Unsafe.As for every type but string. A type wider than the
    // stored value then read past it: a decimal or a Guid was made of whatever lay next to the value in memory,
    // and a reference type such as byte[] ended the process.
    [Fact]
    public void EnumReadsAsACSharpEnumOfAnySize()
    {
        Command.CommandText = "SELECT 'happy'::mood, 'sad'::mood";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<Mood>(0).Should().Be(Mood.Happy);
        reader.GetFieldValue<ByteMood>(0).Should().Be(ByteMood.Happy);
        reader.GetFieldValue<ShortMood>(0).Should().Be(ShortMood.Happy);
        reader.GetFieldValue<LongMood>(0).Should().Be(LongMood.Happy);
        reader.GetFieldValue<LongMood?>(0).Should().Be(LongMood.Happy);

        reader.GetFieldValue<LongMood>(1).Should().Be(LongMood.Sad);
        reader.GetFieldValue<ByteMood>(1).Should().Be(ByteMood.Sad);
    }

    // An integer type gives the position of the value in the list of names of the enum, counted from zero.
    [Fact]
    public void EnumReadsAsItsPositionForAnIntegerType()
    {
        Command.CommandText = "SELECT 'happy'::mood, 'sad'::mood";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetByte(0).Should().Be(2);
        reader.GetInt16(0).Should().Be(2);
        reader.GetInt32(0).Should().Be(2);
        reader.GetInt64(0).Should().Be(2);
        reader.GetFieldValue<sbyte>(0).Should().Be(2);
        reader.GetFieldValue<ulong>(0).Should().Be(2);
        reader.GetFieldValue<int?>(0).Should().Be(2);
        reader.GetFieldValue<BigInteger>(0).Should().Be(new BigInteger(2));

        reader.GetInt32(1).Should().Be(0);
        reader.GetInt64(1).Should().Be(0);
    }

    [Fact]
    public void EnumPositionThatDoesNotFitTheIntegerTypeThrows()
    {
        var names = string.Join(", ", Enumerable.Range(0, 300).Select(i => $"'v{i}'"));

        Command.CommandText = $"CREATE TYPE wide AS ENUM ({names}); SELECT 'v200'::wide AS c";
        using (var reader = Command.ExecuteReader())
        {
            reader.Read();

            reader.GetInt32(0).Should().Be(200);
            reader.GetByte(0).Should().Be(200);
            reader.Invoking(r => r.GetFieldValue<sbyte>(0)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from UInt32 to SByte in column c");
        }

        Command.CommandText = "DROP TYPE wide";
        Command.ExecuteNonQuery();
    }

    [Fact]
    public void EnumReadAsAnUnsupportedTypeThrows()
    {
        Command.CommandText = "SELECT 'happy'::mood AS c";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetFloat(0)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from String to Single in column c");
        reader.Invoking(r => r.GetDouble(0)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from String to Double in column c");
        reader.Invoking(r => r.GetDecimal(0)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from String to Decimal in column c");
        reader.Invoking(r => r.GetBoolean(0)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from String to Boolean in column c");
        reader.Invoking(r => r.GetGuid(0)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from String to Guid in column c");
        reader.Invoking(r => r.GetDateTime(0)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from String to DateTime in column c");
        reader.Invoking(r => r.GetStream(0)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from String to Stream in column c");
        reader.Invoking(r => r.GetFieldValue<byte[]>(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFieldValue<List<int>>(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFieldValue<double?>(0)).Should().Throw<InvalidCastException>();

        reader.GetString(0).Should().Be("happy");
    }

    public enum ByteMood : byte
    {
        Sad, Ok, Happy
    }

    public enum ShortMood : short
    {
        Sad, Ok, Happy
    }

    public enum LongMood : long
    {
        Sad, Ok, Happy
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