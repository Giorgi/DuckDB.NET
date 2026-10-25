using System.Collections;
using System.Collections.ObjectModel;

namespace DuckDB.NET.Test;

// Every element type a list reads on the typed path: each item is read as a column of that type is read with
// GetFieldValue<T>, a NULL item is null in a list of a nullable type and refused in the others, and a List<T> is
// created at its size. The queries have 3,000 rows, so that the second chunk is read too, and each row also
// selects the two items as columns, to compare a list item with the read of a column.
public class ListReadAllElementTypesTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    private const int Rows = 3000;

    public enum Fruit
    {
        Apple, Pear, Plum
    }

    private class DerivedList<T> : List<T>;

    [Fact] public void Booleans() => ReadsValueLists<bool>("BOOLEAN", "{0} % 2 = 0");
    [Fact] public void Bytes() => ReadsValueLists<byte>("UTINYINT", "{0} % 256");
    [Fact] public void SignedBytes() => ReadsValueLists<sbyte>("TINYINT", "{0} % 256 - 128");
    [Fact] public void Shorts() => ReadsValueLists<short>("SMALLINT", "{0} - 1500");
    [Fact] public void UnsignedShorts() => ReadsValueLists<ushort>("USMALLINT", "{0}");
    [Fact] public void Integers() => ReadsValueLists<int>("INTEGER", "{0} * 100000 - 150000000");
    [Fact] public void UnsignedIntegers() => ReadsValueLists<uint>("UINTEGER", "{0} * 1000000");
    [Fact] public void Longs() => ReadsValueLists<long>("BIGINT", "{0} * 1000000000000 - 1500000000000000");
    [Fact] public void UnsignedLongs() => ReadsValueLists<ulong>("UBIGINT", "{0} * 1000000000000000");
    [Fact] public void Floats() => ReadsValueLists<float>("FLOAT", "{0} / 4");
    [Fact] public void Doubles() => ReadsValueLists<double>("DOUBLE", "{0} / 8 - 100");
    [Fact] public void Decimals() => ReadsValueLists<decimal>("DECIMAL(18,3)", "{0} / 1000");
    [Fact] public void WideDecimals() => ReadsValueLists<decimal>("DECIMAL(38,10)", "{0}::DECIMAL(38,10) * 1000000000000 + 0.5");
    [Fact] public void HugeIntsAsBigInteger() => ReadsValueLists<BigInteger>("HUGEINT", "{0}::HUGEINT * 100000000000000000000");
    [Fact] public void UnsignedHugeIntsAsBigInteger() => ReadsValueLists<BigInteger>("UHUGEINT", "{0}::UHUGEINT * 100000000000000000000");
    [Fact] public void VarIntsAsBigInteger() => ReadsValueLists<BigInteger>("VARINT", "{0}::VARINT * 100000000000000000000000000000000000000000");
    [Fact] public void Timestamps() => ReadsValueLists<DateTime>("TIMESTAMP", "TIMESTAMP '2000-01-01' + to_seconds({0} * 1000)");
    [Fact] public void NanosecondTimestamps() => ReadsValueLists<DateTime>("TIMESTAMP_NS", "TIMESTAMP '2000-01-01' + to_seconds({0} * 1000)");
    [Fact] public void TimestampsWithTimeZone() => ReadsValueLists<DateTimeOffset>("TIMESTAMPTZ", "TIMESTAMPTZ '2000-01-01 00:00:00+00' + to_seconds({0} * 1000)");
    [Fact] public void DatesAsDateTime() => ReadsValueLists<DateTime>("DATE", "DATE '2000-01-01' + {0}::INTEGER");
    [Fact] public void Dates() => ReadsValueLists<DateOnly>("DATE", "DATE '2000-01-01' + {0}::INTEGER");
    [Fact] public void Times() => ReadsValueLists<TimeOnly>("TIME", "TIME '00:00:00' + to_seconds({0} * 17)");
    [Fact] public void TimesAsTimeSpan() => ReadsValueLists<TimeSpan>("TIME", "TIME '00:00:00' + to_seconds({0} * 17)");
    [Fact] public void TimesWithTimeZone() => ReadsValueLists<DateTimeOffset>("TIMETZ", "(TIME '00:00:00' + to_seconds({0} * 17))::TIMETZ");
    [Fact] public void Intervals() => ReadsValueLists<TimeSpan>("INTERVAL", "to_seconds({0} * 7) + to_days({0} % 5)");
    [Fact] public void Guids() => ReadsValueLists<Guid>("UUID", "'00000000-0000-0000-0000-' || lpad(CAST({0} AS VARCHAR), 12, '0')");

    [Fact] public void DuckDBDates() => ReadsValueLists<DuckDBDateOnly>("DATE", "DATE '2000-01-01' + {0}::INTEGER");
    [Fact] public void DuckDBTimes() => ReadsValueLists<DuckDBTimeOnly>("TIME", "TIME '00:00:00' + to_seconds({0} * 17)");
    [Fact] public void DuckDBTimesWithTimeZone() => ReadsValueLists<DuckDBTimeTz>("TIMETZ", "(TIME '00:00:00' + to_seconds({0} * 17))::TIMETZ");
    [Fact] public void DuckDBTimestamps() => ReadsValueLists<DuckDBTimestamp>("TIMESTAMP", "TIMESTAMP '2000-01-01' + to_seconds({0} * 1000)");
    [Fact] public void DuckDBIntervals() => ReadsValueLists<DuckDBInterval>("INTERVAL", "to_months({0} % 5) + to_seconds({0} * 7)");
    [Fact] public void DuckDBHugeInts() => ReadsValueLists<DuckDBHugeInt>("HUGEINT", "{0}::HUGEINT * -100000000000000000000");
    [Fact] public void DuckDBUnsignedHugeInts() => ReadsValueLists<DuckDBUHugeInt>("UHUGEINT", "{0}::UHUGEINT * 100000000000000000000");
    [Fact] public void DuckDBDecimals() => ReadsValueLists<DuckDBDecimal>("DECIMAL(18,3)", "{0} / 1000");

    [Fact] public void Strings() => ReadsReferenceLists<string>("VARCHAR", "'s' || {0}", value => value);
    [Fact] public void Blobs() => ReadsReferenceLists<Stream>("BLOB", "('b' || {0})::BLOB", ReadAll);
    [Fact] public void BitStrings() => ReadsReferenceLists<BitArray>("BIT", "('1' || ({0} % 2)::VARCHAR || '01')::BIT", BitsToString);
    [Fact] public void BitStringsAsString() => ReadsReferenceLists<string>("BIT", "('1' || ({0} % 2)::VARCHAR || '01')::BIT", value => value);

    [Fact]
    public void Enums()
    {
        Command.CommandText = "CREATE TYPE fruit AS ENUM ('Apple', 'Pear', 'Plum')";
        Command.ExecuteNonQuery();

        ReadsValueLists<Fruit>("fruit", "(['Apple', 'Pear', 'Plum'])[{0} % 3 + 1]");
        ReadsReferenceLists<string>("fruit", "(['Apple', 'Pear', 'Plum'])[{0} % 3 + 1]", value => value);

        // A list of a C# enum read from integers is refused, as a column of integers is.
        Command.CommandText = "SELECT [1, 2]::INTEGER[], 1::INTEGER";
        using var reader = Command.ExecuteReader();
        reader.Read();
        reader.Invoking(r => r.GetFieldValue<Fruit>(1)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFieldValue<List<Fruit>>(0)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void CapacityIsTheNumberOfItems()
    {
        Command.CommandText = "SELECT range(17)::INTEGER[], range(17)::BIGINT[], [CAST(i AS VARCHAR) FOR i IN range(17)], range(17)::INTEGER[17] AS fixed_size";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var ints = reader.GetFieldValue<List<int>>(0);
        ints.Should().HaveCount(17);
        ints.Capacity.Should().Be(17);

        reader.GetFieldValue<List<long>>(1).Capacity.Should().Be(17);
        reader.GetFieldValue<List<long?>>(1).Capacity.Should().Be(17);
        reader.GetFieldValue<List<short>>(1).Capacity.Should().Be(17);
        reader.GetFieldValue<List<string>>(2).Capacity.Should().Be(17);
        reader.GetFieldValue<List<int>>(3).Capacity.Should().Be(17);
        reader.GetFieldValue<List<long>>(3).Capacity.Should().Be(17);

        var value = reader.GetValue(1).Should().BeOfType<List<long>>().Subject;
        value.Capacity.Should().Be(17);
    }

    [Fact]
    public void NestedLists()
    {
        Command.CommandText = $"SELECT [[i, NULL], NULL, [i + 1]]::INTEGER[][] AS l, [['a' || i], [NULL]] AS s FROM range({Rows}) t(i) ORDER BY i";
        using var reader = Command.ExecuteReader();

        var row = 0;
        while (reader.Read())
        {
            var list = reader.GetFieldValue<List<List<int?>>>(0);
            list.Should().HaveCount(3);
            list[0].Should().Equal(row, null);
            list[1].Should().BeNull();
            list[2].Should().Equal(row + 1);
            list.Capacity.Should().Be(3);

            reader.GetFieldValue<List<List<long?>>>(0)[2].Should().Equal(row + 1L);
            reader.Invoking(r => r.GetFieldValue<List<List<int>>>(0)).Should().Throw<InvalidCastException>().WithMessage("The list contains null value");

            var strings = reader.GetFieldValue<List<List<string>>>(1);
            strings[0].Should().Equal("a" + row);
            strings[1].Should().Equal(new string[] { null });

            row++;
        }

        row.Should().Be(Rows);
    }

    [Fact]
    public void ClassesDerivedFromList()
    {
        Command.CommandText = $"SELECT [i, NULL, i + 1]::INTEGER[] AS l, ['a' || i, NULL] AS s FROM range({Rows}) t(i) ORDER BY i";
        using var reader = Command.ExecuteReader();

        var row = 0;
        while (reader.Read())
        {
            reader.GetFieldValue<DerivedList<long?>>(0).Should().BeOfType<DerivedList<long?>>().And.Equal(row, null, row + 1L);
            reader.GetFieldValue<DerivedList<int?>>(0).Should().BeOfType<DerivedList<int?>>().And.Equal(row, null, row + 1);
            reader.GetFieldValue<DerivedList<string>>(1).Should().BeOfType<DerivedList<string>>().And.Equal("a" + row, null);
            reader.Invoking(r => r.GetFieldValue<DerivedList<long>>(0)).Should().Throw<InvalidCastException>().WithMessage("The list contains null value");

            // A collection that is not a List<T> is filled as before.
            reader.GetFieldValue<Collection<long?>>(0).Should().Equal(row, null, row + 1L);
            reader.GetFieldValue<ObservableCollection<int?>>(0).Should().Equal(row, null, row + 1);

            row++;
        }

        row.Should().Be(Rows);
    }

    // Reads that the object path refused and the typed path, the one of GetFieldValue<T> on a column, allows.
    [Fact]
    public void ItemsConvertAsAColumnDoes()
    {
        Command.CommandText = "CREATE TYPE fruit_index AS ENUM ('Apple', 'Pear', 'Plum')";
        Command.ExecuteNonQuery();

        Command.CommandText = "SELECT [1, -2]::HUGEINT[], [1, 2]::INTEGER[], ['Pear', 'Plum']::fruit_index[], [3, 4]::VARINT[], [5, 6]::UHUGEINT[]";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<List<long>>(0).Should().Equal(1L, -2L);
        reader.GetFieldValue<List<int?>>(0).Should().Equal(1, -2);
        reader.GetFieldValue<List<BigInteger>>(1).Should().Equal(new BigInteger(1), new BigInteger(2));
        reader.GetFieldValue<List<int>>(2).Should().Equal(1, 2);
        reader.GetFieldValue<List<long>>(3).Should().Equal(3L, 4L);
        reader.GetFieldValue<List<ulong>>(4).Should().Equal(5UL, 6UL);
    }

    // An item of a type that the column cannot be read as throws InvalidCastException, as a column does. The object
    // path added the wrong value to the list, which threw ArgumentException.
    [Fact]
    public void AnItemOfTheWrongTypeThrowsInvalidCastException()
    {
        Command.CommandText = "SELECT ['a']::VARCHAR[], [true]::BOOLEAN[], [DATE '2000-01-01'], [1.5]::DECIMAL(18,1)[], [INTERVAL 1 DAY], [1, 2]::HUGEINT[]";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetFieldValue<List<int>>(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFieldValue<List<int>>(1)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFieldValue<List<TimeOnly>>(2)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFieldValue<List<long>>(3)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFieldValue<List<int>>(4)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFieldValue<List<double>>(5)).Should().Throw<InvalidCastException>();
    }

    private void ReadsValueLists<T>(string duckDBType, string valueSql) where T : struct
    {
        Command.CommandText = Query(duckDBType, valueSql);
        using var reader = Command.ExecuteReader();

        var row = 0;
        while (reader.Read())
        {
            var first = reader.GetFieldValue<T>(3);
            var second = reader.GetFieldValue<T>(4);

            ShouldBe(reader.GetFieldValue<List<T?>>(0), [first, null, second], row);
            ShouldBe(reader.GetFieldValue<List<T>>(1), [first, second], row);
            ShouldBe(reader.GetFieldValue<List<T?>>(1), [first, second], row);
            ShouldBe(reader.GetFieldValue<List<T>>(2), [first, second, first], row);
            ShouldBe(reader.GetFieldValue<List<T?>>(2), [first, second, first], row);

            if (row == 0)
            {
                reader.Invoking(r => r.GetFieldValue<List<T>>(0)).Should().Throw<InvalidCastException>().WithMessage("The list contains null value");
            }

            row++;
        }

        row.Should().Be(Rows);
    }

    private void ReadsReferenceLists<T>(string duckDBType, string valueSql, Func<T, string> text) where T : class
    {
        Command.CommandText = Query(duckDBType, valueSql);
        using var reader = Command.ExecuteReader();

        var row = 0;
        while (reader.Read())
        {
            var first = text(reader.GetFieldValue<T>(3));
            var second = text(reader.GetFieldValue<T>(4));

            ShouldBe(reader.GetFieldValue<List<T>>(0).Select(item => item == null ? null : text(item)).ToList(), [first, null, second], row);
            ShouldBe(reader.GetFieldValue<List<T>>(1).Select(text).ToList(), [first, second], row);
            ShouldBe(reader.GetFieldValue<List<T>>(2).Select(text).ToList(), [first, second, first], row);

            reader.GetFieldValue<List<T>>(1).Capacity.Should().Be(2);

            row++;
        }

        row.Should().Be(Rows);
    }

    // Columns: a list with a NULL item, a list without one, a fixed-size ARRAY, and the two items as columns.
    private static string Query(string duckDBType, string valueSql)
    {
        var first = string.Format(valueSql, "i");
        var second = string.Format(valueSql, "(i + 1)");

        return $"""
                SELECT [a, NULL, b]::{duckDBType}[] AS with_null, [a, b]::{duckDBType}[] AS full_list,
                       [a, b, a]::{duckDBType}[3] AS fixed_size, a, b
                FROM (SELECT i, ({first})::{duckDBType} AS a, ({second})::{duckDBType} AS b FROM range({Rows}) t(i))
                ORDER BY i
                """;
    }

    // FluentAssertions is slow for 3,000 rows of every type, so it is called only when the lists differ.
    private static void ShouldBe<TItem>(List<TItem> actual, TItem[] expected, int row)
    {
        if (!actual.SequenceEqual(expected))
        {
            actual.Should().Equal(expected, $"row {row}");
        }

        actual.Capacity.Should().Be(expected.Length, $"row {row}");
    }

    private static string ReadAll(Stream stream)
    {
        using var memory = new MemoryStream();
        stream.CopyTo(memory);
        return Convert.ToBase64String(memory.ToArray());
    }

    private static string BitsToString(BitArray bits)
    {
        var builder = new StringBuilder(bits.Length);

        foreach (bool bit in bits)
        {
            builder.Append(bit ? '1' : '0');
        }

        return builder.ToString();
    }
}
