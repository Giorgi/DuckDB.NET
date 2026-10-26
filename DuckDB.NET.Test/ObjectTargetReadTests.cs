using System.Collections;
using System.Globalization;
using DuckDB.NET.Data.DataChunk.Reader;

namespace DuckDB.NET.Test;

// object as the requested type means the column's own .NET type, whatever reads it: GetFieldValue<object> and
// GetValue on a column, a struct field read into an object property (of a class or a C# struct), the input of a
// scalar function read with GetValue<object>, and GetValue(offset, typeof(object)) on a reader, which is how a struct
// field is read where no code can be made at run time. A list of object and a dictionary with object keys or values
// stay refused, with their own message.
public class ObjectTargetReadTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    public static readonly string[] Expressions =
    [
        "42::TINYINT", "42::INTEGER", "42::UBIGINT", "1.5::DOUBLE", "42::HUGEINT", "42::UHUGEINT", "42::VARINT",
        "1.25::DECIMAL(9,2)", "1.25::DECIMAL(38,4)", "'b'::ENUM('a', 'b')", "true", "'x'::VARCHAR", "'101'::BIT",
        "'\\x01\\x02'::BLOB", "'00000000-0000-0000-0000-000000000001'::UUID",
        "DATE '2024-01-02'", "TIMESTAMP '2024-01-02 03:04:05.123456'", "TIMESTAMP_S '2024-01-02 03:04:05'",
        "TIMESTAMP_MS '2024-01-02 03:04:05.123'", "TIMESTAMP_NS '2024-01-02 03:04:05.123456789'",
        "TIMESTAMPTZ '2024-01-02 03:04:05+00'", "TIME '03:04:05'", "TIMETZ '03:04:05+01'", "INTERVAL 1 DAY",
        "[1, 2]::INTEGER[]", "[1, 2]::INTEGER[2]", "{'a': 1, 'b': 'x'}", "MAP {1: 'x'}", "[{'a': DATE '2024-01-02'}]",
        "{'d': DATE '2024-01-02', 'l': [TIME '01:02:03']}",
    ];

    public static IEnumerable<object[]> Cases() => Expressions.Select(expression => new object[] { expression });

    private class Holder
    {
        public object V { get; set; }
    }

    private struct StructHolder
    {
        public object V { get; set; }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void StructFieldReadAsObjectIsTheColumnValue(string expression)
    {
        Command.CommandText = $"SELECT {expression} AS c, {{'v': {expression}}} AS s";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var column = Describe(reader.GetFieldValue<object>(0));

        Describe(reader.GetValue(0)).Should().Be(column);
        Describe(reader.GetFieldValue<Holder>(1).V).Should().Be(column, $"{expression} as a field of a class");
        Describe(reader.GetFieldValue<StructHolder>(1).V).Should().Be(column, $"{expression} as a field of a C# struct");
        Describe(reader.GetFieldValue<Dictionary<string, object>>(1)["v"]).Should().Be(column, $"{expression} in a dictionary");
    }

    // The reader of the input of a scalar function is the reader a column or a struct field of that type gets. Through
    // it the test calls GetValue(offset, typeof(object)) directly, the route of a struct field read without dynamic
    // code, which a test process cannot take otherwise.
    [Fact]
    public void ReadersGiveTheColumnValueForObject()
    {
        Connection.RegisterScalarFunction<object, string>("describe_as_object", (readers, writer, rowCount) =>
        {
            var input = readers[0];

            for (ulong index = 0; index < rowCount; index++)
            {
                var asObject = ((VectorDataReaderBase)input).GetValue(index, typeof(object));

                writer.WriteValue($"{Describe(input.GetValue(index))}|{Describe(asObject)}|{Describe(input.GetValue<object>(index))}", index);
            }
        });

        foreach (var expression in Expressions)
        {
            Command.CommandText = $"SELECT {expression} AS c, describe_as_object({expression}) AS d";
            using var reader = Command.ExecuteReader();
            reader.Read();

            var column = Describe(reader.GetFieldValue<object>(0));
            var outcomes = reader.GetString(1).Split('|');

            outcomes[0].Should().Be(column, $"{expression}: GetValue(offset) of the input");
            outcomes[1].Should().Be(column, $"{expression}: GetValue(offset, typeof(object)) of the input");
            outcomes[2].Should().Be(column, $"{expression}: GetValue<object>(offset) of the input");
        }
    }

    [Fact]
    public void ListsAndMapsOfObjectStayRefused()
    {
        Command.CommandText = "SELECT [1, 2]::INTEGER[] AS l, MAP {1: 'x'} AS m";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.Invoking(r => r.GetFieldValue<List<object>>(0)).Should().Throw<InvalidCastException>().WithMessage("*as a list of object*");
        reader.Invoking(r => r.GetFieldValue<Dictionary<int, object>>(1)).Should().Throw<InvalidCastException>().WithMessage("*object keys or values*");
    }

    // A value as its type and its text; a list, a dictionary and a stream by their items, entries and length.
    internal static string Describe(object value) => value switch
    {
        null => "null",
        string text => $"String {text}",
        Stream stream => $"{TypeName(value.GetType())} {stream.Length}",
        IDictionary dictionary => $"{TypeName(value.GetType())} {{{string.Join(", ", dictionary.Keys.Cast<object>().Select(key => $"{Describe(key)}: {Describe(dictionary[key])}"))}}}",
        IEnumerable items => $"{TypeName(value.GetType())} [{string.Join(", ", items.Cast<object>().Select(Describe))}]",
        _ => $"{TypeName(value.GetType())} {Convert.ToString(value, CultureInfo.InvariantCulture)}",
    };

    private static string TypeName(Type type) => type.IsGenericType
        ? $"{type.Name[..type.Name.IndexOf('`')]}<{string.Join(", ", type.GetGenericArguments().Select(TypeName))}>"
        : type.Name;
}
