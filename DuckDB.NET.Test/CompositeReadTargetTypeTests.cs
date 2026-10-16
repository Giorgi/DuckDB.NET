namespace DuckDB.NET.Test;

// The list, struct and map readers work out what to do with a target type once, when it is first asked for, and
// keep it for the rows after. These tests read one column as different types in turn, and as a class derived from
// a list type, so that what was worked out for one type is never used for another.
public class CompositeReadTargetTypeTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    private class DerivedIntList : List<int>;

    private class DerivedList<T> : List<T>;

    private class IntAndText
    {
        public int A { get; set; }
        public string B { get; set; }
    }

    private class OnlyALong
    {
        public long A { get; set; }
    }

    [Fact]
    public void AListColumnReadAsDifferentTypesInTurn()
    {
        Command.CommandText = "SELECT [i, i + 1, NULL]::INTEGER[] AS l, [i, i + 1]::INTEGER[] AS full_list FROM range(3) t(i) ORDER BY i";
        using var reader = Command.ExecuteReader();

        var row = 0;
        while (reader.Read())
        {
            reader.GetFieldValue<List<int?>>(0).Should().Equal(row, row + 1, null);
            reader.GetFieldValue<List<long?>>(0).Should().Equal(row, row + 1, null);
            reader.GetFieldValue<List<int?>>(0).Should().Equal(row, row + 1, null);

            // A NULL item in a list of a type that cannot hold it is refused, on the typed path and the other.
            reader.Invoking(r => r.GetFieldValue<List<int>>(0)).Should().Throw<InvalidCastException>().WithMessage("The list contains null value");
            reader.Invoking(r => r.GetFieldValue<List<long>>(0)).Should().Throw<InvalidCastException>().WithMessage("The list contains null value");

            reader.GetFieldValue<List<int>>(1).Should().Equal(row, row + 1);
            reader.GetFieldValue<List<long>>(1).Should().Equal(row, row + 1);
            reader.GetFieldValue<List<double>>(1).Should().Equal(row, row + 1);
            reader.GetFieldValue<List<int>>(1).Should().Equal(row, row + 1);

            // A class derived from a list type is created as itself. One that is not generic is refused.
            reader.GetFieldValue<DerivedList<int>>(1).Should().BeOfType<DerivedList<int>>().And.Equal(row, row + 1);
            reader.GetFieldValue<DerivedList<long>>(1).Should().BeOfType<DerivedList<long>>().And.Equal(row, row + 1);
            reader.Invoking(r => r.GetFieldValue<DerivedIntList>(1)).Should().Throw<InvalidCastException>().WithMessage("Cannot read column full_list as DerivedIntList*");

            row++;
        }

        row.Should().Be(3);
    }

    [Fact]
    public void AStructColumnReadAsDifferentTypesInTurn()
    {
        Command.CommandText = "SELECT {'a': i::INTEGER, 'b': 'x' || i} AS s FROM range(3) t(i) ORDER BY i";
        using var reader = Command.ExecuteReader();

        var row = 0;
        while (reader.Read())
        {
            var first = reader.GetFieldValue<IntAndText>(0);
            first.A.Should().Be(row);
            first.B.Should().Be("x" + row);

            reader.GetFieldValue<OnlyALong>(0).A.Should().Be(row);

            reader.GetFieldValue<Dictionary<string, object>>(0).Should().BeEquivalentTo(new Dictionary<string, object> { ["a"] = row, ["b"] = "x" + row });

            var again = reader.GetFieldValue<IntAndText>(0);
            again.A.Should().Be(row);
            again.B.Should().Be("x" + row);

            row++;
        }

        row.Should().Be(3);
    }

    [Fact]
    public void AMapColumnReadAsDifferentTypesInTurn()
    {
        Command.CommandText = "SELECT MAP {i::INTEGER: 'v' || i, (i + 10)::INTEGER: NULL} AS m FROM range(3) t(i) ORDER BY i";
        using var reader = Command.ExecuteReader();

        var row = 0;
        while (reader.Read())
        {
            reader.GetFieldValue<Dictionary<int, string>>(0).Should().BeEquivalentTo(new Dictionary<int, string> { [row] = "v" + row, [row + 10] = null });
            reader.GetFieldValue<Dictionary<long, string>>(0).Should().BeEquivalentTo(new Dictionary<long, string> { [row] = "v" + row, [row + 10] = null });
            reader.GetFieldValue<Dictionary<int, string>>(0).Should().BeEquivalentTo(new Dictionary<int, string> { [row] = "v" + row, [row + 10] = null });

            row++;
        }

        row.Should().Be(3);
    }
}
