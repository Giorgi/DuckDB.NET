using System.Collections.Generic;
using System.Numerics;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBDataReaderTestAllTypes : DuckDBTestBase
{
    private readonly DuckDBDataReader reader;

    public DuckDBDataReaderTestAllTypes(DuckDBDatabaseFixture db) : base(db)
    {
        Command.CommandText = "Select * from test_all_types()";
        reader = Command.ExecuteReader();
        reader.Read();
    }

    private void VerifyData<T>(string columnName, int columnIndex, IReadOnlyList<T?> data) where T : struct
    {
        reader.GetOrdinal(columnName).Should().Be(columnIndex);

        reader.GetValue(columnIndex).Should().Be(data[0]);
        reader.GetFieldValue<T>(columnIndex).Should().Be(data[0]);

        reader.Read();

        reader.GetValue(columnIndex).Should().Be(data[1]);
        reader.GetFieldValue<T>(columnIndex).Should().Be(data[1]);

        reader.Read();

        reader.IsDBNull(columnIndex).Should().Be(true);
        reader.GetFieldValue<T?>(columnIndex).Should().Be(null);
    }

    [Fact]
    public void ReadBool()
    {
        VerifyData<bool>("bool", 0, new List<bool?> { false, true, null });
    }

    [Fact]
    public void ReadTinyInt()
    {
        VerifyData<sbyte>("tinyint", 1, new List<sbyte?> { -128, 127, null });
    }

    [Fact]
    public void ReadSmallInt()
    {
        VerifyData<short>("smallint", 2, new List<short?> { -32768, 32767, null });
    }

    [Fact]
    public void ReadInt()
    {
        VerifyData<int>("int", 3, new List<int?> { -2147483648, 2147483647, null });
    }

    [Fact]
    public void ReadBigInt()
    {
        VerifyData<long>("bigint", 4, new List<long?> { -9223372036854775808, 9223372036854775807, null });
    }

    [Fact]
    public void ReadHugeInt()
    {
        VerifyData<BigInteger>("hugeint", 5, new List<BigInteger?>
        {
            BigInteger.Parse("-170141183460469231731687303715884105727"), 
            BigInteger.Parse("170141183460469231731687303715884105727"), 
            null
        });
    }

    [Fact]
    public void ReadUTinyInt()
    {
        VerifyData<byte>("utinyint", 6, new List<byte?> { 0, 255, null });
    }

    [Fact]
    public void ReadUSmallInt()
    {
        VerifyData<ushort>("usmallint", 7, new List<ushort?> { 0, 65535, null });
    }

    [Fact]
    public void ReadUInt()
    {
        VerifyData<uint>("uint", 8, new List<uint?> { 0, 4294967295, null });
    }

    [Fact]
    public void ReadUBigInt()
    {
        VerifyData<ulong>("ubigint", 9, new List<ulong?> { 0, 18446744073709551615, null });
    }
}