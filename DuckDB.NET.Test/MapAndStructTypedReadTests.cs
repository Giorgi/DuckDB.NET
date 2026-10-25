namespace DuckDB.NET.Test;

// MAP read into a Dictionary<TKey, TValue> and STRUCT read into a class or a C# struct read their keys, values and
// fields on the typed path: the conversion of GetFieldValue<T> on a column. These tests cover named and other key and
// value types, NULL values, more than one chunk (2048 rows), classes derived from Dictionary<,>, and what a struct
// field is read into.
public class MapAndStructTypedReadTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    private const int Rows = 3000;

    [Fact]
    public void MapsOfSeveralKeyAndValueTypesOverMoreThanOneChunk()
    {
        Command.CommandText = $$"""
            SELECT i,
                   MAP {i::INTEGER: 'v' || i, (i + 100000)::INTEGER: NULL} AS int_text,
                   MAP {'a' || i: i::BIGINT, 'b' || i: NULL::BIGINT} AS text_long,
                   MAP {i::BIGINT: i / 4} AS long_double,
                   MAP {i::INTEGER: i::INTEGER} AS int_int,
                   MAP {i::SMALLINT % 1000: (i / 100)::DECIMAL(10, 2)} AS small_decimal,
                   MAP {'k': TIMESTAMP '2024-01-01' + INTERVAL (i) SECOND} AS text_timestamp,
                   MAP {'k': [i, i + 1]::INTEGER[]} AS text_list,
                   MAP {'k': {'a': i::INTEGER, 'b': 'x' || i} } AS text_struct
            FROM range({{Rows}}) t(i) ORDER BY i
            """;
        using var reader = Command.ExecuteReader();

        var row = 0;
        while (reader.Read())
        {
            reader.GetFieldValue<Dictionary<int, string>>(1).Should().Equal(new Dictionary<int, string> { [row] = "v" + row, [row + 100000] = null });
            reader.GetFieldValue<Dictionary<string, long?>>(2).Should().Equal(new Dictionary<string, long?> { ["a" + row] = row, ["b" + row] = null });
            reader.GetFieldValue<Dictionary<string, double?>>(2).Should().Equal(new Dictionary<string, double?> { ["a" + row] = row, ["b" + row] = null });
            reader.GetFieldValue<Dictionary<long, double>>(3).Should().Equal(new Dictionary<long, double> { [row] = row / 4.0 });
            reader.GetFieldValue<Dictionary<int, long>>(4).Should().Equal(new Dictionary<int, long> { [row] = row });
            reader.GetFieldValue<Dictionary<long, int>>(4).Should().Equal(new Dictionary<long, int> { [row] = row });
            reader.GetFieldValue<Dictionary<int, double>>(4).Should().Equal(new Dictionary<int, double> { [row] = row });
            reader.GetFieldValue<Dictionary<short, decimal>>(5).Should().Equal(new Dictionary<short, decimal> { [(short)(row % 1000)] = row / 100m });
            reader.GetFieldValue<Dictionary<string, DateTime>>(6).Should().Equal(new Dictionary<string, DateTime> { ["k"] = new DateTime(2024, 1, 1).AddSeconds(row) });
            reader.GetFieldValue<Dictionary<string, DateTimeOffset>>(6).Should().Equal(new Dictionary<string, DateTimeOffset> { ["k"] = new DateTimeOffset(new DateTime(2024, 1, 1).AddSeconds(row), TimeSpan.Zero) });

            var lists = reader.GetFieldValue<Dictionary<string, List<long>>>(7);
            lists.Should().ContainSingle().Which.Value.Should().Equal(row, row + 1);

            var structs = reader.GetFieldValue<Dictionary<string, IntAndText>>(8);
            structs.Should().ContainSingle();
            structs["k"].A.Should().Be(row);
            structs["k"].B.Should().Be("x" + row);

            // GetValue reads the column's own types, and gives the same.
            reader.GetValue(1).Should().BeOfType<Dictionary<int, string>>().Which.Should().Equal(new Dictionary<int, string> { [row] = "v" + row, [row + 100000] = null });
            reader.GetValue(3).Should().BeOfType<Dictionary<long, double>>().Which.Should().Equal(new Dictionary<long, double> { [row] = row / 4.0 });

            row++;
        }

        row.Should().Be(Rows);
    }

    [Fact]
    public void MapsWithUuidKeysAndBooleanTimestampAndUuidValues()
    {
        Command.CommandText = $$"""
            SELECT i,
                   MAP {u: 'v' || i} AS uuid_text,
                   MAP {u: i % 2 = 0, other: NULL} AS uuid_bool,
                   MAP {i::INTEGER: TIMESTAMP '2024-01-01' + INTERVAL (i) SECOND, (i + 100000)::INTEGER: NULL} AS int_timestamp,
                   MAP {'k' || i: u, 'n' || i: NULL::UUID} AS text_uuid
            FROM (SELECT i,
                         ('00000000-0000-0000-0000-' || printf('%012x', i))::UUID AS u,
                         ('00000000-0000-0000-0001-' || printf('%012x', i))::UUID AS other
                  FROM range({{Rows}}) t(i))
            ORDER BY i
            """;
        using var reader = Command.ExecuteReader();

        var row = 0;
        while (reader.Read())
        {
            var key = Guid.Parse($"00000000-0000-0000-0000-{row:x12}");
            var otherKey = Guid.Parse($"00000000-0000-0000-0001-{row:x12}");
            var timestamp = new DateTime(2024, 1, 1).AddSeconds(row);

            reader.GetFieldValue<Dictionary<Guid, string>>(1).Should().Equal(new Dictionary<Guid, string> { [key] = "v" + row });
            reader.GetFieldValue<Dictionary<Guid, bool?>>(2).Should().Equal(new Dictionary<Guid, bool?> { [key] = row % 2 == 0, [otherKey] = null });
            reader.GetFieldValue<Dictionary<int, DateTime?>>(3).Should().Equal(new Dictionary<int, DateTime?> { [row] = timestamp, [row + 100000] = null });
            reader.GetFieldValue<Dictionary<string, Guid?>>(4).Should().Equal(new Dictionary<string, Guid?> { ["k" + row] = key, ["n" + row] = null });

            reader.Invoking(r => r.GetFieldValue<Dictionary<Guid, bool>>(2)).Should().Throw<InvalidCastException>();
            reader.Invoking(r => r.GetFieldValue<Dictionary<int, DateTime>>(3)).Should().Throw<InvalidCastException>();
            reader.Invoking(r => r.GetFieldValue<Dictionary<string, Guid>>(4)).Should().Throw<InvalidCastException>();

            row++;
        }

        row.Should().Be(Rows);
    }

    [Fact]
    public void MapWithANullValueIntoADictionaryOfAValueTypeIsRefused()
    {
        Command.CommandText = "SELECT MAP {1: 10, 2: NULL} AS m";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetFieldValue<Dictionary<int, int>>(0)).Should().Throw<InvalidCastException>()
              .WithMessage("The Map in column m contains null value but dictionary does not allow null values");
        reader.Invoking(r => r.GetFieldValue<Dictionary<int, short>>(0)).Should().Throw<InvalidCastException>()
              .WithMessage("The Map in column m contains null value but dictionary does not allow null values");

        reader.GetFieldValue<Dictionary<int, int?>>(0).Should().Equal(new Dictionary<int, int?> { [1] = 10, [2] = null });
        reader.GetFieldValue<Dictionary<int, short?>>(0).Should().Equal(new Dictionary<int, short?> { [1] = 10, [2] = null });
    }

    [Fact]
    public void MapValueThatDoesNotFitIsRefused()
    {
        Command.CommandText = "SELECT MAP {'k': 300} AS m";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetFieldValue<Dictionary<string, byte>>(0)).Should().Throw<InvalidCastException>()
              .WithMessage("Cannot cast from Int32 to Byte in column m");
    }

    [Fact]
    public void MapOfTheWrongTypeIsRefusedWithInvalidCastException()
    {
        Command.CommandText = "SELECT MAP {'k': 'text'} AS m";
        using var reader = Command.ExecuteReader();
        reader.Read();

        // Through IDictionary this was an ArgumentException from the dictionary.
        reader.Invoking(r => r.GetFieldValue<Dictionary<string, int>>(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFieldValue<Dictionary<string, Guid>>(0)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void MapReadsThatTheObjectPathRefused()
    {
        Command.CommandText = "SELECT MAP {'k': 5} AS i, MAP {'k': 5::HUGEINT} AS h, MAP {'k': 'b'::ENUM('a', 'b')} AS e";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<Dictionary<string, BigInteger>>(0).Should().Equal(new Dictionary<string, BigInteger> { ["k"] = 5 });
        reader.GetFieldValue<Dictionary<string, long>>(1).Should().Equal(new Dictionary<string, long> { ["k"] = 5 });
        reader.GetFieldValue<Dictionary<string, int>>(2).Should().Equal(new Dictionary<string, int> { ["k"] = 1 });
        reader.GetFieldValue<Dictionary<string, string>>(2).Should().Equal(new Dictionary<string, string> { ["k"] = "b" });
    }

    [Fact]
    public void MapIntoAClassDerivedFromDictionary()
    {
        Command.CommandText = $"SELECT MAP {{'key' || i: i::INTEGER, 'null' || i: NULL}} AS m FROM range({Rows}) t(i) ORDER BY i";
        using var reader = Command.ExecuteReader();

        var row = 0;
        while (reader.Read())
        {
            var generic = reader.GetFieldValue<CustomDictionary<string, int?>>(0);
            generic.Should().BeOfType<CustomDictionary<string, int?>>();
            generic.Should().Equal(new Dictionary<string, int?> { ["key" + row] = row, ["null" + row] = null });

            // A class that is not generic itself. The key and value types are those of its base class.
            var derived = reader.GetFieldValue<TextToNullableLong>(0);
            derived.Should().BeOfType<TextToNullableLong>();
            derived.Should().Equal(new Dictionary<string, long?> { ["key" + row] = row, ["null" + row] = null });

            reader.Invoking(r => r.GetFieldValue<CustomDictionary<string, int>>(0)).Should().Throw<InvalidCastException>()
                  .WithMessage("The Map in column m contains null value but dictionary does not allow null values");

            row++;
        }

        row.Should().Be(Rows);
    }

    [Fact]
    public void MapIntoAClassDerivedFromDictionaryIsReadByItsBase()
    {
        Command.CommandText = $"SELECT MAP {{'key' || i: i::INTEGER, 'null' || i: NULL}} AS m FROM range({Rows}) t(i) ORDER BY i";
        using var reader = Command.ExecuteReader();

        var row = 0;
        while (reader.Read())
        {
            // The first type argument is not the key type: the key and value types are those of the base class.
            reader.GetFieldValue<Tagged<object, long?>>(0).Should().Equal(new Dictionary<string, long?> { ["key" + row] = row, ["null" + row] = null });
            reader.GetFieldValue<Tagged<int, long?>>(0).Should().Equal(new Dictionary<string, long?> { ["key" + row] = row, ["null" + row] = null });

            // Object values are refused, also when only the base class names them.
            reader.Invoking(r => r.GetFieldValue<TextToObject>(0)).Should().Throw<InvalidCastException>()
                  .WithMessage("Cannot read Map column m as a dictionary with object keys or values*");

            row++;
        }

        row.Should().Be(Rows);
    }

    [Fact]
    public void MapIntoTypesThatAreNotADictionaryOfConcreteTypes()
    {
        Command.CommandText = "SELECT MAP {2: 'b', 1: 'a', 3: NULL} AS m";
        using var reader = Command.ExecuteReader();
        reader.Read();

        // Filled through IDictionary, as before.
        reader.GetFieldValue<SortedDictionary<int, string>>(0).Should().Equal(new SortedDictionary<int, string> { [1] = "a", [2] = "b", [3] = null });
        reader.GetFieldValue<Dictionary<int?, string>>(0).Should().Equal(new Dictionary<int?, string> { [1] = "a", [2] = "b", [3] = null });

        reader.Invoking(r => r.GetFieldValue<Dictionary<int, object>>(0)).Should().Throw<InvalidCastException>();
        reader.Invoking(r => r.GetFieldValue<List<int>>(0)).Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void StructIntoClassWithOtherNumericTypes()
    {
        Command.CommandText = $$"""
            SELECT {'a': i::INTEGER, 'b': (i / 2)::FLOAT, 'c': (i / 4)::DOUBLE, 'd': (i % 100)::TINYINT, 'e': i::INTEGER, 'f': i::HUGEINT, 'g': i::UBIGINT} AS s
            FROM range({{Rows}}) t(i) ORDER BY i
            """;
        using var reader = Command.ExecuteReader();

        var row = 0;
        while (reader.Read())
        {
            var value = reader.GetFieldValue<OtherNumericTypes>(0);

            value.A.Should().Be(row);
            value.B.Should().Be(row / 2.0);
            value.C.Should().Be(row / 4m);
            value.D.Should().Be(row % 100);
            value.E.Should().Be(new BigInteger(row));
            value.F.Should().Be(row);
            value.G.Should().Be(row);

            row++;
        }

        row.Should().Be(Rows);
    }

    [Fact]
    public void StructFieldThatDoesNotFitIsRefused()
    {
        Command.CommandText = "SELECT {'a': 300} AS s";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetFieldValue<ByteProperty>(0)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from Int32 to Byte in column s");
    }

    [Fact]
    public void StructIntoClassWithNullableProperties()
    {
        Command.CommandText = $$"""
            SELECT {'a': CASE WHEN i % 3 = 0 THEN NULL ELSE i::INTEGER END,
                     'b': CASE WHEN i % 5 = 0 THEN NULL ELSE i::INTEGER END,
                     'c': CASE WHEN i % 7 = 0 THEN NULL ELSE 'x' || i END,
                     'd': CASE WHEN i % 2 = 0 THEN NULL ELSE i::INTEGER END} AS s
            FROM range({{Rows}}) t(i) ORDER BY i
            """;
        using var reader = Command.ExecuteReader();

        var row = 0;
        while (reader.Read())
        {
            var value = reader.GetFieldValue<NullableProperties>(0);

            value.A.Should().Be(row % 3 == 0 ? null : row);
            value.B.Should().Be(row % 5 == 0 ? null : row);
            value.C.Should().Be(row % 7 == 0 ? null : "x" + row);
            // A NULL field leaves a property that can hold null as it is: here, at its initial value.
            value.D.Should().Be(row % 2 == 0 ? -1 : row);

            row++;
        }

        row.Should().Be(Rows);
    }

    [Fact]
    public void StructWithNullIntoNonNullablePropertyIsRefused()
    {
        Command.CommandText = "SELECT {'a': 1, 'b': NULL::INTEGER} AS s";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetFieldValue<TwoInts>(0)).Should().Throw<InvalidCastException>().WithMessage("Property 'B' is not nullable but struct contains null value");
        reader.Invoking(r => r.GetFieldValue<TwoIntsStruct>(0)).Should().Throw<InvalidCastException>().WithMessage("Property 'B' is not nullable but struct contains null value");
    }

    [Fact]
    public void StructWithMissingAndExtraProperties()
    {
        Command.CommandText = "SELECT {'A': 1, 'extra': 'ignored', 'NAME': 'n'} AS s";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var value = reader.GetFieldValue<MissingAndExtra>(0);

        // Fields are matched to properties by name, ignoring case. A property with no field keeps its value; a field
        // with no property is not read.
        value.A.Should().Be(1);
        value.Name.Should().Be("n");
        value.Missing.Should().Be("initial");
        value.ReadOnly.Should().Be(0);
    }

    [Fact]
    public void StructWithInheritedPrivateAndInitSetters()
    {
        Command.CommandText = "SELECT {'base': 1, 'private': 2, 'init': 3} AS s";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var value = reader.GetFieldValue<DerivedWithSetters>(0);

        value.Base.Should().Be(1);
        value.Private.Should().Be(2);
        value.Init.Should().Be(3);
    }

    [Fact]
    public void StructWithNestedStructListAndMapFields()
    {
        Command.CommandText = $$"""
            SELECT {'id': i::INTEGER,
                     'inner': CASE WHEN i % 4 = 0 THEN NULL ELSE {'a': i::INTEGER, 'b': 'x' || i} END,
                     'items': [i, i + 1]::INTEGER[],
                     'inners': [{'a': i::INTEGER, 'b': 'y'}, NULL],
                     'tags': MAP {'t': i::INTEGER},
                     'point': {'x': i::INTEGER, 'y': (i + 1)::INTEGER, 'name': 'p', 'z': NULL::INTEGER} } AS s
            FROM range({{Rows}}) t(i) ORDER BY i
            """;
        using var reader = Command.ExecuteReader();

        var row = 0;
        while (reader.Read())
        {
            var value = reader.GetFieldValue<Nested>(0);

            value.Id.Should().Be(row);

            if (row % 4 == 0)
            {
                value.Inner.Should().BeNull();
            }
            else
            {
                value.Inner.A.Should().Be(row);
                value.Inner.B.Should().Be("x" + row);
            }

            value.Items.Should().Equal(row, row + 1);
            value.Inners.Should().HaveCount(2);
            value.Inners[0].A.Should().Be(row);
            value.Inners[0].B.Should().Be("y");
            value.Inners[1].Should().BeNull();
            value.Tags.Should().Equal(new Dictionary<string, long> { ["t"] = row });
            value.Point.Should().Be(new Point { X = row, Y = row + 1, Name = "p", Z = null });

            row++;
        }

        row.Should().Be(Rows);
    }

    [Fact]
    public void StructIntoACSharpStruct()
    {
        Command.CommandText = $$"""
            SELECT CASE WHEN i % 10 = 0 THEN NULL ELSE {'x': i::INTEGER, 'y': (i * 2)::INTEGER, 'name': 'p' || i, 'z': CASE WHEN i % 3 = 0 THEN NULL ELSE i END} END AS s,
                   [{'x': i::INTEGER, 'y': 1, 'name': NULL, 'z': 7}] AS l
            FROM range({{Rows}}) t(i) ORDER BY i
            """;
        using var reader = Command.ExecuteReader();

        var row = 0;
        while (reader.Read())
        {
            if (row % 10 == 0)
            {
                reader.IsDBNull(0).Should().BeTrue();
                reader.GetFieldValue<Point?>(0).Should().BeNull();
            }
            else
            {
                var expected = new Point { X = row, Y = row * 2, Name = "p" + row, Z = row % 3 == 0 ? null : row };

                reader.GetFieldValue<Point>(0).Should().Be(expected);
                reader.GetFieldValue<Point?>(0).Should().Be(expected);

                // The same column as a class and as a struct in turn.
                reader.GetFieldValue<PointClass>(0).X.Should().Be(row);
                reader.GetFieldValue<Point>(0).Should().Be(expected);
            }

            reader.GetFieldValue<List<Point>>(1).Should().Equal(new Point { X = row, Y = 1, Name = null, Z = 7 });

            row++;
        }

        row.Should().Be(Rows);
    }

    [Fact]
    public void AnExceptionOfASetterIsNotWrapped()
    {
        Command.CommandText = "SELECT {'a': 1} AS s";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetFieldValue<ThrowingSetter>(0)).Should().Throw<FormatException>().WithMessage("boom");
        reader.Invoking(r => r.GetFieldValue<ThrowingSetterStruct>(0)).Should().Throw<FormatException>().WithMessage("boom");
    }

    private class ThrowingSetter
    {
        public int A { get => 0; set => throw new FormatException("boom"); }
    }

    private struct ThrowingSetterStruct
    {
        public int A { get => 0; set => throw new FormatException("boom"); }
    }

    private class IntAndText
    {
        public int A { get; set; }
        public string B { get; set; }
    }

    private sealed class CustomDictionary<TKey, TValue> : Dictionary<TKey, TValue>;

    private sealed class TextToNullableLong : Dictionary<string, long?>;

    private sealed class Tagged<TTag, TValue> : Dictionary<string, TValue>;

    private sealed class TextToObject : Dictionary<string, object>;

    private class OtherNumericTypes
    {
        public long A { get; set; }
        public double B { get; set; }
        public decimal C { get; set; }
        public int D { get; set; }
        public BigInteger E { get; set; }
        public long F { get; set; }
        public int G { get; set; }
    }

    private class ByteProperty
    {
        public byte A { get; set; }
    }

    private class NullableProperties
    {
        public int? A { get; set; }
        public long? B { get; set; }
        public string C { get; set; }
        public int? D { get; set; } = -1;
    }

    private class TwoInts
    {
        public int A { get; set; }
        public int B { get; set; }
    }

    private struct TwoIntsStruct
    {
        public int A { get; set; }
        public int B { get; set; }
    }

    private class MissingAndExtra
    {
        public int A { get; set; }
        public string Name { get; set; }
        public string Missing { get; set; } = "initial";
        public int ReadOnly { get; }
    }

    private class BaseWithSetter
    {
        public int Base { get; set; }
    }

    private class DerivedWithSetters : BaseWithSetter
    {
        public int Private { get; private set; }
        public int Init { get; init; }
    }

    private class Nested
    {
        public int Id { get; set; }
        public IntAndText Inner { get; set; }
        public List<int> Items { get; set; }
        public List<IntAndText> Inners { get; set; }
        public Dictionary<string, long> Tags { get; set; }
        public Point Point { get; set; }
    }

    private record struct Point
    {
        public int X { get; set; }
        public int Y { get; set; }
        public string Name { get; set; }
        public int? Z { get; set; }
    }

    private class PointClass
    {
        public int X { get; set; }
    }
}