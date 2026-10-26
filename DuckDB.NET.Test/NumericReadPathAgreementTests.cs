using System.Collections.ObjectModel;
using System.Globalization;
using System.Reflection;

namespace DuckDB.NET.Test;

// A number is read on one of two paths. A column, and a list item of type int, float, double or decimal, are read
// on the typed path (GetFieldValue<T>). Every other list item, a map key or value and a struct field read into a
// class are read with the target type known only at run time. Both must convert with the same rules, so that a
// value reads the same wherever it is stored.
//
// A FLOAT, DOUBLE or DECIMAL with a fraction, read as an integer type, is refused on every path (2.0 item 5; before
// 2.0 a FLOAT or DOUBLE was rounded half to even and a DECIMAL was refused whatever its value).
public class NumericReadPathAgreementTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    private static readonly string[] Columns =
    [
        "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
        "FLOAT", "DOUBLE", "HUGEINT", "UHUGEINT", "VARINT",
        "DECIMAL(4,1)", "DECIMAL(9,2)", "DECIMAL(18,3)", "DECIMAL(38,4)",
    ];

    private static readonly string[] Values =
    [
        "0", "42", "-1", "255", "300", "70000", "5000000000", "100000000000000000000",
        "1.5", "2.5", "-2.5", "0.5", "-0.5",
    ];

    private static readonly Type[] Targets =
    [
        typeof(sbyte), typeof(short), typeof(int), typeof(long), typeof(byte), typeof(ushort), typeof(uint), typeof(ulong),
        typeof(float), typeof(double), typeof(decimal), typeof(BigInteger), typeof(string),
        typeof(DuckDBHugeInt), typeof(DuckDBUHugeInt), typeof(DuckDBDecimal), typeof(IntEnum),
    ];

    private static readonly MethodInfo ReadOnEveryPathMethod = typeof(NumericReadPathAgreementTests).GetMethod(nameof(ReadOnEveryPath), BindingFlags.NonPublic | BindingFlags.Static);
    private static readonly MethodInfo ReadOnEveryNullablePathMethod = typeof(NumericReadPathAgreementTests).GetMethod(nameof(ReadOnEveryNullablePath), BindingFlags.NonPublic | BindingFlags.Static);

    private class Holder<T>
    {
        public T V { get; set; }
    }

    private struct StructHolder
    {
        public object V { get; set; }
    }

    // A struct field read into an object property of a C# struct, which the struct reader sets through another kind
    // of property reader than a class's.
    private static string StructFieldAsObject(DuckDBDataReader reader) => Outcome(() => reader.GetFieldValue<StructHolder>(2).V);

    private enum IntEnum
    {
        Zero = 0,
    }

    private enum SByteEnum : sbyte
    {
        Zero = 0,
    }

    private enum ByteEnum : byte
    {
        Zero = 0,
    }

    private enum LongEnum : long
    {
        Zero = 0,
    }

    // The targets of an ENUM column: its name, its position as an integer type and as a C# enum of each size, and
    // types it refuses.
    private static readonly Type[] EnumTargets =
    [
        typeof(sbyte), typeof(short), typeof(int), typeof(long), typeof(byte), typeof(ushort), typeof(uint), typeof(ulong),
        typeof(BigInteger), typeof(string), typeof(SByteEnum), typeof(ByteEnum), typeof(IntEnum), typeof(LongEnum),
        typeof(float), typeof(double), typeof(decimal), typeof(DuckDBHugeInt),
    ];

    // An enum of 3 names is stored in 1 byte, one of 300 names in 2 bytes, one of 70,000 names in 4 bytes.
    public static IEnumerable<object[]> EnumCases()
    {
        foreach (var position in new[] { 0, 1, 2 })
        {
            yield return [3, position];
        }

        foreach (var position in new[] { 0, 127, 128, 255, 256, 299 })
        {
            yield return [300, position];
        }

        foreach (var position in new[] { 32768, 65536 })
        {
            yield return [70000, position];
        }
    }

    public static IEnumerable<object[]> Cases()
    {
        foreach (var column in Columns)
        {
            foreach (var value in Values)
            {
                yield return [column, value];
            }
        }
    }

    // One value, read as every target type: from a column, as a list item, as a struct field, as a map value and as
    // a map key. Each read gives the same value of the same type, or each throws the same exception type.
    [Theory]
    [MemberData(nameof(Cases))]
    public void EveryPathReadsANumberLikeTheColumn(string column, string literal)
    {
        using var reader = ReadOnEveryPathQuery($"TRY_CAST({literal} AS {column})");

        // TRY_CAST gives NULL when the column type cannot hold the value; such a case has nothing to read.
        if (!reader.Read())
        {
            return;
        }

        foreach (var target in Targets)
        {
            var outcomes = (string[])ReadOnEveryPathMethod.MakeGenericMethod(target).Invoke(null, [reader]);
            var onColumn = outcomes[0];

            // A DuckDB struct reads only from its own column type, and a C# enum from no numeric column. Every path
            // could agree on a wrong value here, so the column is checked on its own.
            if (IsRefusedByEveryColumnExceptItsOwn(column, target))
            {
                onColumn.Should().Be(nameof(InvalidCastException), $"{literal} as {column} read as {target.Name}: the column");
            }

            outcomes[1].Should().Be(onColumn, $"{literal} as {column} read as {target.Name}: a list item");
            outcomes[2].Should().Be(onColumn, $"{literal} as {column} read as {target.Name}: a struct field");
            outcomes[3].Should().Be(onColumn, $"{literal} as {column} read as {target.Name}: a map value");
            outcomes[4].Should().Be(onColumn, $"{literal} as {column} read as {target.Name}: a map key");
            outcomes[5].Should().Be(onColumn, $"{literal} as {column} read as {target.Name}: a Collection item");

            // The same through the nullable forms, which the list, struct and map readers unwrap to the type under
            // them. A map key cannot be null, so it has no nullable form here.
            if (target.IsValueType)
            {
                var nullable = (string[])ReadOnEveryNullablePathMethod.MakeGenericMethod(target).Invoke(null, [reader]);

                nullable[0].Should().Be(onColumn, $"{literal} as {column} read as {target.Name}?: the column");
                nullable[1].Should().Be(onColumn, $"{literal} as {column} read as {target.Name}?: a list item");
                nullable[2].Should().Be(onColumn, $"{literal} as {column} read as {target.Name}?: a struct field");
                nullable[3].Should().Be(onColumn, $"{literal} as {column} read as {target.Name}?: a map value");
                nullable[4].Should().Be(onColumn, $"{literal} as {column} read as {target.Name}?: a Collection item");
            }
        }

        // object: the column gives the value in its own .NET type, and so does a struct field read into an object
        // property, of a class or of a C# struct. A list of object and a dictionary with object keys or values are
        // refused on purpose, with their own message.
        var asObject = ReadOnEveryPath<object>(reader);

        asObject[0].Should().NotBe(nameof(InvalidCastException), $"{literal} as {column} read as object: the column");
        asObject[1].Should().Be(nameof(InvalidCastException), $"{literal} as {column} read as object: a list item");
        asObject[2].Should().Be(asObject[0], $"{literal} as {column} read as object: a struct field");
        StructFieldAsObject(reader).Should().Be(asObject[0], $"{literal} as {column} read as object: a field of a C# struct");
        asObject[3].Should().Be(nameof(InvalidCastException), $"{literal} as {column} read as object: a map value");
        asObject[4].Should().Be(nameof(InvalidCastException), $"{literal} as {column} read as object: a map key");
        asObject[5].Should().Be(nameof(InvalidCastException), $"{literal} as {column} read as object: a Collection item");
    }

    // An ENUM value read as every target type, on every path, as the numbers are above. The column reads as its name
    // (string), as its position (an integer type, if it fits) and as a C# enum of any size, and refuses the rest.
    [Theory]
    [MemberData(nameof(EnumCases))]
    public void EveryPathReadsAnEnumLikeTheColumn(int names, int position)
    {
        var enumType = names > 1000 ? CreateLargeEnumType(names) : $"ENUM({string.Join(", ", Enumerable.Range(0, names).Select(i => $"'v{i}'"))})";
        using var reader = ReadOnEveryPathQuery($"'v{position}'::{enumType}");
        reader.Read().Should().BeTrue();

        foreach (var target in EnumTargets)
        {
            var outcomes = (string[])ReadOnEveryPathMethod.MakeGenericMethod(target).Invoke(null, [reader]);
            var onColumn = outcomes[0];

            onColumn.Should().Be(ExpectedEnumOutcome(target, position), $"ENUM of {names} names at {position} read as {target.Name}: the column");

            outcomes[1].Should().Be(onColumn, $"ENUM of {names} names at {position} read as {target.Name}: a list item");
            outcomes[2].Should().Be(onColumn, $"ENUM of {names} names at {position} read as {target.Name}: a struct field");
            outcomes[3].Should().Be(onColumn, $"ENUM of {names} names at {position} read as {target.Name}: a map value");
            outcomes[4].Should().Be(onColumn, $"ENUM of {names} names at {position} read as {target.Name}: a map key");
            outcomes[5].Should().Be(onColumn, $"ENUM of {names} names at {position} read as {target.Name}: a Collection item");

            if (target.IsValueType)
            {
                var nullable = (string[])ReadOnEveryNullablePathMethod.MakeGenericMethod(target).Invoke(null, [reader]);

                nullable.Should().AllBe(onColumn, $"ENUM of {names} names at {position} read as {target.Name}?");
            }
        }

        // object: the column gives the name, and so does a struct field read into an object property. A list and a
        // map of object are refused on purpose.
        var asObject = ReadOnEveryPath<object>(reader);

        asObject[0].Should().Be($"String v{position}");
        asObject[2].Should().Be(asObject[0], $"ENUM of {names} names at {position} read as object: a struct field");
        StructFieldAsObject(reader).Should().Be(asObject[0], $"ENUM of {names} names at {position} read as object: a field of a C# struct");
        new[] { asObject[1], asObject[3], asObject[4], asObject[5] }.Should().AllBe(nameof(InvalidCastException), $"ENUM of {names} names at {position} read as object");
    }

    private static string ExpectedEnumOutcome(Type target, int position)
    {
        if (target == typeof(string)) return $"String v{position}";
        if (target.IsEnum) return $"{target.Name} {Enum.ToObject(target, position)}";
        if (target == typeof(BigInteger)) return $"BigInteger {position}";

        if (target == typeof(sbyte)) return position <= sbyte.MaxValue ? $"SByte {position}" : nameof(InvalidCastException);
        if (target == typeof(byte)) return position <= byte.MaxValue ? $"Byte {position}" : nameof(InvalidCastException);
        if (target == typeof(short)) return position <= short.MaxValue ? $"Int16 {position}" : nameof(InvalidCastException);
        if (target == typeof(ushort)) return position <= ushort.MaxValue ? $"UInt16 {position}" : nameof(InvalidCastException);
        if (target == typeof(int) || target == typeof(uint) || target == typeof(long) || target == typeof(ulong))
        {
            return $"{target.Name} {position}";
        }

        return nameof(InvalidCastException);
    }

    // An enum of more than 65,535 names, too long to write out in the query. Created once per database.
    private string CreateLargeEnumType(int names)
    {
        var name = $"enum_of_{names}";

        Command.CommandText = $"SELECT count(*) FROM duckdb_types() WHERE type_name = '{name}'";

        if (Convert.ToInt64(Command.ExecuteScalar(), CultureInfo.InvariantCulture) == 0)
        {
            Command.CommandText = $"CREATE TYPE {name} AS ENUM (SELECT 'v' || i FROM range({names}) t(i))";
            Command.ExecuteNonQuery();
        }

        return name;
    }

    [Fact]
    public void StructFieldsReadIntoPropertiesOfOtherNumericTypes()
    {
        Command.CommandText = """
            SELECT {'tiny': 1::TINYINT, 'int': 2::INTEGER, 'big': 3::BIGINT, 'ubig': 4::UBIGINT, 'huge': 5::HUGEINT,
                    'uhuge': 6::UHUGEINT, 'var': 7::VARINT, 'dbl': 8.0::DOUBLE, 'flt': 9.0::FLOAT, 'dec': 10.25::DECIMAL(9,2)} AS s
            """;
        using var reader = Command.ExecuteReader();
        reader.Read();

        var fields = reader.GetFieldValue<OtherNumericTypes>(0);

        fields.Tiny.Should().Be(1L);
        fields.Int.Should().Be(new BigInteger(2));
        fields.Big.Should().Be((short)3);
        fields.UBig.Should().Be(4m);
        fields.Huge.Should().Be(5L);
        fields.UHuge.Should().Be((byte)6);
        fields.Var.Should().Be(7);
        fields.Dbl.Should().Be(8L);
        fields.Flt.Should().Be(9.0);
        fields.Dec.Should().Be(10.25m);
    }

    private class OtherNumericTypes
    {
        public long Tiny { get; set; }
        public BigInteger Int { get; set; }
        public short Big { get; set; }
        public decimal UBig { get; set; }
        public long? Huge { get; set; }
        public byte UHuge { get; set; }
        public int? Var { get; set; }
        public long Dbl { get; set; }
        public double Flt { get; set; }
        public decimal Dec { get; set; }
    }

    [Fact]
    public void StructFieldThatDoesNotFitThePropertyThrows()
    {
        Command.CommandText = "SELECT {'v': 70000::BIGINT} AS big, {'v': 100000000000000000000::HUGEINT} AS huge, {'v': 1.25::DECIMAL(9,2)} AS dec";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetFieldValue<Holder<short>>(0)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from Int64 to Int16 in column big");
        reader.Invoking(r => r.GetFieldValue<Holder<long>>(1)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from HugeInt to Int64 in column huge");

        // A DECIMAL with a fraction is refused as an integer type, and a DECIMAL as a floating point type, as the column
        // does.
        reader.Invoking(r => r.GetFieldValue<Holder<int>>(2)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast a value with a non-zero fractional part to Int32 in column dec");
        reader.Invoking(r => r.GetFieldValue<Holder<double>>(2)).Should().Throw<InvalidCastException>();
        reader.GetFieldValue<Holder<decimal>>(2).V.Should().Be(1.25m);
        reader.GetFieldValue<Holder<decimal?>>(2).V.Should().Be(1.25m);
        reader.GetFieldValue<Holder<object>>(2).V.Should().Be(1.25m);
    }

    [Fact]
    public void MapKeysAndValuesReadAsOtherNumericTypes()
    {
        Command.CommandText = """
            SELECT MAP {1::HUGEINT: 10::INTEGER} AS huge_int,
                   MAP {2::UBIGINT: 2.5::DOUBLE} AS ubig_double,
                   MAP {3::VARINT: 4::SMALLINT} AS var_small,
                   MAP {5::INTEGER: 6.75::DECIMAL(9,2)} AS int_dec
            """;
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<Dictionary<long, BigInteger>>(0).Should().Equal(new Dictionary<long, BigInteger> { [1] = 10 });
        reader.GetFieldValue<Dictionary<int, long?>>(0).Should().Equal(new Dictionary<int, long?> { [1] = 10 });
        reader.GetFieldValue<Dictionary<byte, decimal>>(0).Should().Equal(new Dictionary<byte, decimal> { [1] = 10m });

        reader.GetFieldValue<Dictionary<BigInteger, decimal>>(1).Should().Equal(new Dictionary<BigInteger, decimal> { [2] = 2.5m });
        // A fraction is refused, as a DOUBLE column read as int refuses it.
        reader.Invoking(r => r.GetFieldValue<Dictionary<int, int>>(1)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast a value with a non-zero fractional part to Int32 in column ubig_double");

        reader.GetFieldValue<Dictionary<short, BigInteger>>(2).Should().Equal(new Dictionary<short, BigInteger> { [3] = 4 });

        reader.GetFieldValue<Dictionary<BigInteger, decimal>>(3).Should().Equal(new Dictionary<BigInteger, decimal> { [5] = 6.75m });
        reader.Invoking(r => r.GetFieldValue<Dictionary<int, int>>(3)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast a value with a non-zero fractional part to Int32 in column int_dec");
    }

    [Fact]
    public void ListsOfElementTypesReadOnTheObjectPath()
    {
        Command.CommandText = """
            SELECT [1, 2, 3]::INTEGER[] AS ints,
                   [1, 2, NULL]::HUGEINT[] AS huge,
                   [4, 5]::UHUGEINT[] AS uhuge,
                   [6, -7]::VARINT[] AS var,
                   [1.5, 2.5, -2.5]::DOUBLE[] AS doubles,
                   [0.5, 3.5]::FLOAT[] AS floats,
                   [1.00, 2.50]::DECIMAL(9,2)[] AS decs,
                   [300, 2]::HUGEINT[] AS too_big,
                   [2.0, -3.0]::DOUBLE[] AS whole_doubles,
                   [1.00, -2.00]::DECIMAL(9,2)[] AS whole_decs
            """;
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<List<long>>(0).Should().Equal(1L, 2L, 3L);
        reader.GetFieldValue<List<BigInteger>>(0).Should().Equal(new BigInteger(1), new BigInteger(2), new BigInteger(3));
        reader.GetFieldValue<List<BigInteger?>>(0).Should().Equal(new BigInteger(1), new BigInteger(2), new BigInteger(3));
        reader.GetFieldValue<List<short>>(0).Should().Equal((short)1, (short)2, (short)3);

        reader.GetFieldValue<List<long?>>(1).Should().Equal(1L, 2L, null);
        reader.GetFieldValue<List<int?>>(1).Should().Equal(1, 2, null);
        reader.GetFieldValue<List<BigInteger?>>(1).Should().Equal(new BigInteger(1), new BigInteger(2), null);

        reader.GetFieldValue<List<ulong>>(2).Should().Equal(4UL, 5UL);
        reader.GetFieldValue<List<long>>(3).Should().Equal(6L, -7L);

        // A fraction is refused, as a column refuses it; a whole value reads.
        reader.Invoking(r => r.GetFieldValue<List<long>>(4)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast a value with a non-zero fractional part to Int64 in column doubles");
        reader.Invoking(r => r.GetFieldValue<List<short>>(5)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast a value with a non-zero fractional part to Int16 in column floats");
        reader.Invoking(r => r.GetFieldValue<List<BigInteger>>(4)).Should().Throw<InvalidCastException>();
        reader.GetFieldValue<List<long>>(8).Should().Equal(2L, -3L);
        reader.GetFieldValue<Collection<int?>>(8).Should().Equal(2, -3);

        reader.GetFieldValue<List<decimal>>(6).Should().Equal(1.00m, 2.50m);
        reader.Invoking(r => r.GetFieldValue<List<long>>(6)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast a value with a non-zero fractional part to Int64 in column decs");
        reader.Invoking(r => r.GetFieldValue<List<BigInteger>>(6)).Should().Throw<InvalidCastException>();
        reader.GetFieldValue<List<long>>(9).Should().Equal(1L, -2L);
        reader.GetFieldValue<List<BigInteger>>(9).Should().Equal(new BigInteger(1), new BigInteger(-2));
        reader.GetFieldValue<Collection<short>>(9).Should().Equal((short)1, (short)-2);

        reader.GetFieldValue<List<short>>(7).Should().Equal((short)300, (short)2);
        reader.Invoking(r => r.GetFieldValue<List<byte>>(7)).Should().Throw<InvalidCastException>().WithMessage("Cannot cast from HugeInt to Byte in column too_big");
    }

    // The column's own type and the provider-specific type still read as before on the object path.
    [Fact]
    public void NaturalAndProviderSpecificTypesAreUnchanged()
    {
        Command.CommandText = "SELECT [1]::HUGEINT[] AS huge, [2]::UHUGEINT[] AS uhuge, [3]::INTEGER[] AS ints, [4.5]::DECIMAL(9,2)[] AS decs";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetValue(0).Should().BeOfType<List<BigInteger>>().Which.Should().Equal(new BigInteger(1));
        reader.GetProviderSpecificValue(0).Should().BeOfType<List<DuckDBHugeInt>>().Which.Single().ToBigInteger().Should().Be(1);
        reader.GetProviderSpecificValue(1).Should().BeOfType<List<DuckDBUHugeInt>>().Which.Single().ToBigInteger().Should().Be(2);
        reader.GetValue(2).Should().BeOfType<List<int>>().Which.Should().Equal(3);
        reader.GetValue(3).Should().BeOfType<List<decimal>>().Which.Should().Equal(4.5m);
        reader.GetProviderSpecificValue(3).Should().BeOfType<List<DuckDBDecimal>>();
    }

    private DuckDBDataReader ReadOnEveryPathQuery(string expression)
    {
        Command.CommandText = $$"""
            WITH t AS (SELECT {{expression}} AS c)
            SELECT c, [c] AS list, {'v': c} AS struct, MAP {1::INTEGER: c} AS map_value, MAP {c: 1::INTEGER} AS map_key
            FROM t WHERE c IS NOT NULL
            """;

        return Command.ExecuteReader();
    }

    // The outcome of reading the row as T on each path: the column, a list item, a struct field, a map value, a map
    // key and a Collection item. A value is its type and its text, an exception is its type. A List<T> may read its
    // items on the typed path; a Collection<T> reads them through GetValue(offset, Type), the object path.
    private static string[] ReadOnEveryPath<T>(DuckDBDataReader reader)
    {
        return
        [
            Outcome(() => reader.GetFieldValue<T>(0)),
            Outcome(() => reader.GetFieldValue<List<T>>(1)[0]),
            Outcome(() => reader.GetFieldValue<Holder<T>>(2).V),
            Outcome(() => reader.GetFieldValue<Dictionary<int, T>>(3)[1]),
            Outcome(() => reader.GetFieldValue<Dictionary<T, int>>(4).Keys.Single()),
            Outcome(() => reader.GetFieldValue<Collection<T>>(1)[0]),
        ];
    }

    private static bool IsRefusedByEveryColumnExceptItsOwn(string column, Type target)
    {
        if (target == typeof(DuckDBHugeInt)) return column != "HUGEINT";
        if (target == typeof(DuckDBUHugeInt)) return column != "UHUGEINT";
        if (target == typeof(DuckDBDecimal)) return !column.StartsWith("DECIMAL", StringComparison.Ordinal);
        return target == typeof(IntEnum);
    }

    // The same with T? as the requested type: the column, a list item, a struct field, a map value and a Collection
    // item.
    private static string[] ReadOnEveryNullablePath<T>(DuckDBDataReader reader) where T : struct
    {
        return
        [
            Outcome(() => reader.GetFieldValue<T?>(0)),
            Outcome(() => reader.GetFieldValue<List<T?>>(1)[0]),
            Outcome(() => reader.GetFieldValue<Holder<T?>>(2).V),
            Outcome(() => reader.GetFieldValue<Dictionary<int, T?>>(3)[1]),
            Outcome(() => reader.GetFieldValue<Collection<T?>>(1)[0]),
        ];
    }

    private static string Outcome<T>(Func<T> read)
    {
        try
        {
            object value = read();
            return $"{value.GetType().Name} {Convert.ToString(value, CultureInfo.InvariantCulture)}";
        }
        catch (Exception exception)
        {
            return exception.GetType().Name;
        }
    }
}
