namespace DuckDB.NET.Test;

// A byte[] written by a scalar or a table function goes to a BLOB column as its bytes. A byte[] is also an ICollection,
// and before it had a case of its own the writer took it for a list and failed (issue #395).
public class BlobFunctionWriteTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    // Empty, inline (up to 12 bytes) and not inline, with a zero byte inside.
    private static readonly byte[][] Values =
    [
        [1, 2, 3],
        [],
        null,
        [0, 255, 0, 128],
        Enumerable.Range(0, 12).Select(i => (byte)i).ToArray(),
        Enumerable.Range(0, 13).Select(i => (byte)(255 - i)).ToArray(),
        Enumerable.Range(0, 5000).Select(i => (byte)(i * 7)).ToArray(),
    ];

    [Fact]
    [Trait("V2", "Ready")]
    public void AScalarFunctionReturnsAByteArray()
    {
        Connection.RegisterScalarFunction("make_blob", () => new byte[] { 1, 2, 3 });

        Command.CommandText = "SELECT make_blob()";
        Command.ExecuteScalar().Should().BeEquivalentTo(new byte[] { 1, 2, 3 });

        Command.CommandText = "SELECT typeof(make_blob()), hex(make_blob())";
        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetString(0).Should().Be("BLOB");
        reader.GetString(1).Should().Be("010203");
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void AScalarFunctionReturnsEmptyNullAndLongByteArrays()
    {
        Connection.RegisterScalarFunction<int, byte[]>("blob_at", index => Values[index]);

        Command.CommandText = $"SELECT blob_at(i::INT) FROM range({Values.Length}) t(i) ORDER BY i";
        Check();
    }

    [Fact]
    [Trait("V2", "Ready")]
    public void ATableFunctionWritesAByteArrayColumn()
    {
        Connection.RegisterTableFunction("blob_rows",
            () => new TableFunction([new ColumnInfo("data", typeof(byte[]))], Values.Select(value => new Box(value))),
            (item, writers, rowIndex) => writers[0].WriteValue(((Box)item!).Value, rowIndex));

        Command.CommandText = "SELECT data FROM blob_rows()";
        Check();
    }

    private void Check()
    {
        using var reader = Command.ExecuteReader();

        foreach (var value in Values)
        {
            reader.Read().Should().BeTrue();

            if (value == null)
            {
                reader.IsDBNull(0).Should().BeTrue();
            }
            else
            {
                reader.IsDBNull(0).Should().BeFalse();
                reader.GetFieldValue<byte[]>(0).Should().Equal(value);
            }
        }

        reader.Read().Should().BeFalse();
    }

    // A table function's rows are objects, and a null row is not something the enumerable should hold.
    private sealed record Box(byte[] Value);
}
