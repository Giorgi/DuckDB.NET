using DuckDB.NET.Test.Helpers;

namespace DuckDB.NET.Test.Parameters;

public class NamedParameterTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact] // EC1 — https://github.com/Giorgi/DuckDB.NET/issues/203
    public void BindsFromDollarPrefixedParameterName()
    {
        Command.CommandText = "SELECT $PARM1::INT";
        Command.Parameters.Add(new DuckDBParameter("$PARM1", 42));

        Command.ExecuteScalar().Should().Be(42);
    }

    // EC2 — '$' is the only marker DuckDB has, so it is the only prefix stripped. '@' is the one
    // people actually type out of SQL Server habit; the rest guard against generalising the strip
    // to any leading punctuation.
    [Theory]
    [InlineData("@name")]
    [InlineData("?name")]
    [InlineData(":name")]
    public void ThrowsForAnyParameterNamePrefixOtherThanDollar(string parameterName)
    {
        Command.CommandText = "SELECT $name::INT";
        Command.Parameters.Add(new DuckDBParameter(parameterName, 42));

        Command.Invoking(command => command.ExecuteScalar())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*name*");
    }

    [Fact] // EC3
    public void BindsFromUnprefixedParameterName()
    {
        Command.CommandText = "SELECT $name::INT";
        Command.Parameters.Add(new DuckDBParameter("name", 42));

        Command.ExecuteScalar().Should().Be(42);
    }

    [Fact] // EC4
    public void PrefersExactMatchWhenBothPrefixedAndUnprefixedEntriesExist()
    {
        Command.CommandText = "SELECT $id::INT";
        Command.Parameters.Add(new DuckDBParameter("$id", 24));
        Command.Parameters.Add(new DuckDBParameter("id", 42));

        Command.ExecuteScalar().Should().Be(42);
    }

    [Fact] // EC5 — regression against collapsing the two matching passes into one
    public void BindsQuotedNameThatItselfStartsWithDollar()
    {
        Command.CommandText = """SELECT $"$foo"::INT""";
        Command.Parameters.Add(new DuckDBParameter("$foo", 42));

        Command.ExecuteScalar().Should().Be(42);
    }

    // A quoted marker can declare a name that starts with '@', which is why '@' must not be a
    // stripped prefix: stripping it would let an entry named "@foo" answer for a declared "foo".
    [Fact]
    public void BindsQuotedNameThatItselfStartsWithAt()
    {
        Command.CommandText = """SELECT $"@foo"::INT""";
        Command.Parameters.Add(new DuckDBParameter("@foo", 42));

        Command.ExecuteScalar().Should().Be(42);
    }

    [Fact] // EC6
    public void BindsQuotedNameConsistingOfDigits()
    {
        Command.CommandText = """SELECT $"1"::INT""";
        Command.Parameters.Add(new DuckDBParameter("1", 42));

        Command.ExecuteScalar().Should().Be(42);
    }

    [Fact] // EC7
    public void BindsPositionalStatementWhenEveryEntryCarriesAnOrdinalName()
    {
        Command.CommandText = "SELECT ?::INT - ?::INT";
        Command.Parameters.Add(new DuckDBParameter("1", 42));
        Command.Parameters.Add(new DuckDBParameter("2", 24));

        Command.ExecuteScalar().Should().Be(18);
    }

    [Fact] // EC8
    public void BindsNamesDifferingOnlyInCaseAsOneParameter()
    {
        Command.CommandText = "SELECT $Id::INT + $id::INT";
        Command.Parameters.Add(new DuckDBParameter("Id", 21));

        Command.ExecuteScalar().Should().Be(42);
    }

    [Fact] // EC9
    public void BindsRepeatedNameIntoEveryPosition()
    {
        Command.CommandText = "SELECT $name::INT + $name::INT";
        Command.Parameters.Add(new DuckDBParameter("name", 21));

        Command.ExecuteScalar().Should().Be(42);
    }

    [Fact] // EC10
    public void BindsEachStatementOfAMultiStatementCommandFromOneCollection()
    {
        using var defer = new Defer(() => Connection.Execute("DROP TABLE NamedParameterMultiStatement;"));

        Connection.Execute("CREATE TABLE NamedParameterMultiStatement (Value INTEGER);");

        Command.CommandText = """
                              INSERT INTO NamedParameterMultiStatement VALUES ($first::INT);
                              INSERT INTO NamedParameterMultiStatement VALUES ($second::INT);
                              """;
        Command.Parameters.Add(new DuckDBParameter("first", 42));
        Command.Parameters.Add(new DuckDBParameter("second", 24));
        Command.ExecuteNonQuery();

        Connection.Query<int>("SELECT Value FROM NamedParameterMultiStatement ORDER BY Value;")
            .Should().Equal(24, 42);
    }

    [Fact] // EC11 — the defect being fixed: this used to bind nothing and report nothing
    public void ThrowsWhenNoEntryMatchesADeclaredName()
    {
        Command.CommandText = "SELECT $name::INT";
        Command.Parameters.Add(new DuckDBParameter("other", 42));

        Command.Invoking(command => command.ExecuteScalar())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*name*");
    }

    [Fact] // EC12
    public void ThrowsWhenTheOnlyCandidateDiffersInCase()
    {
        Command.CommandText = "SELECT $name::INT";
        Command.Parameters.Add(new DuckDBParameter("Name", 42));

        Command.Invoking(command => command.ExecuteScalar())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("*name*");
    }

    [Fact] // EC13
    public void ThrowsCountGuardWhenFewerEntriesThanDeclaredParameters()
    {
        Command.CommandText = "SELECT $first::INT + $second::INT";
        Command.Parameters.Add(new DuckDBParameter("first", 42));

        Command.Invoking(command => command.ExecuteScalar())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("Invalid number of parameters. Expected 2, got 1");
    }

    [Fact] // EC14
    public void IgnoresAnEntryMatchingNothingDeclared()
    {
        Command.CommandText = "SELECT $used::INT";
        Command.Parameters.Add(new DuckDBParameter("unused", 24));
        Command.Parameters.Add(new DuckDBParameter("used", 42));

        Command.ExecuteScalar().Should().Be(42);
    }

    [Theory] // EC15
    [InlineData(null)]
    [InlineData("")]
    public void IgnoresAnEntryWithNoParameterName(string parameterName)
    {
        Command.CommandText = "SELECT $used::INT";
        Command.Parameters.Add(new DuckDBParameter(parameterName, 24));
        Command.Parameters.Add(new DuckDBParameter("used", 42));

        Command.ExecuteScalar().Should().Be(42);
    }

    [Fact] // EC16
    public void IgnoresAnEntryNamedDollarOnly()
    {
        Command.CommandText = "SELECT $foo::INT";
        Command.Parameters.Add(new DuckDBParameter("$", 24));
        Command.Parameters.Add(new DuckDBParameter("foo", 42));

        Command.ExecuteScalar().Should().Be(42);
    }

    [Fact]
    public void IndexOfFindsAPrefixedEntryByItsBareName()
    {
        var parameters = new DuckDBParameterCollection { new DuckDBParameter("$id", 42) };

        parameters.IndexOf("id").Should().Be(0);
    }

    [Fact]
    public void IndexOfPrefersTheExactMatch()
    {
        var parameters = new DuckDBParameterCollection
        {
            new DuckDBParameter("$id", 24),
            new DuckDBParameter("id", 42)
        };

        parameters.IndexOf("id").Should().Be(1);
    }

    [Fact]
    public void IndexOfDoesNotFindAnAtPrefixedEntryByItsBareName()
    {
        var parameters = new DuckDBParameterCollection { new DuckDBParameter("@id", 42) };

        parameters.IndexOf("id").Should().Be(-1);
    }

    [Fact]
    public void IndexOfReturnsMinusOneWhenNeitherFormIsPresent()
    {
        var parameters = new DuckDBParameterCollection { new DuckDBParameter("$id", 42) };

        parameters.IndexOf("other").Should().Be(-1);
    }

    // Contains, RemoveAt(string) and this[string] all resolve through IndexOf, so each inherits the
    // prefix-stripping pass and each is asserted against a '$'-prefixed entry.
    [Fact]
    public void ContainsFindsAPrefixedEntryByItsBareName()
    {
        var parameters = new DuckDBParameterCollection { new DuckDBParameter("$id", 42) };

        parameters.Contains("id").Should().BeTrue();
        parameters.Contains("other").Should().BeFalse();
    }

    [Fact]
    public void RemoveAtRemovesAPrefixedEntryByItsBareName()
    {
        var parameters = new DuckDBParameterCollection { new DuckDBParameter("$id", 42) };

        parameters.RemoveAt("id");

        parameters.Count.Should().Be(0);
    }

    [Fact]
    public void IndexerReadsAPrefixedEntryByItsBareName()
    {
        var parameters = new DuckDBParameterCollection { new DuckDBParameter("$id", 42) };

        parameters["id"].Value.Should().Be(42);
    }

    [Fact]
    public void RebindsTheSameCommandWhenAParameterValueChanges()
    {
        Command.CommandText = "SELECT $name::INT";
        Command.Parameters.Add(new DuckDBParameter("name", 42));

        Command.ExecuteScalar().Should().Be(42);

        Command.Parameters[0].Value = 24;

        Command.ExecuteScalar().Should().Be(24);
    }

    [Theory] // I7 — the positional branch is unchanged
    [InlineData("SELECT ?::INT - ?::INT")]
    [InlineData("SELECT $1::INT - $2::INT")]
    public void BindsPositionallyFromUnnamedParameters(string query)
    {
        Command.CommandText = query;
        Command.Parameters.Add(new DuckDBParameter(42));
        Command.Parameters.Add(new DuckDBParameter(24));

        Command.ExecuteScalar().Should().Be(18);
    }
}
