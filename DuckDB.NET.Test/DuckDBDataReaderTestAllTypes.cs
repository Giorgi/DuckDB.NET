using System;
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
        Command.CommandText = "Select * from test_all_types(use_large_enum = true)";
        reader = Command.ExecuteReader();
        reader.Read();
    }

    private void VerifyDataStruct<T>(string columnName, int columnIndex, IReadOnlyList<T> data) where T : struct
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

    private void VerifyDataClass<T>(string columnName, int columnIndex, IReadOnlyList<T> data) where T : class
    {
        reader.GetOrdinal(columnName).Should().Be(columnIndex);

        reader.GetValue(columnIndex).Should().Be(data[0]);
        reader.GetFieldValue<T>(columnIndex).Should().Be(data[0]);

        reader.Read();

        reader.GetValue(columnIndex).Should().Be(data[1]);
        reader.GetFieldValue<T>(columnIndex).Should().Be(data[1]);

        reader.Read();

        reader.IsDBNull(columnIndex).Should().Be(true);
    }

    private void VerifyDataList<T>(string columnName, int columnIndex, IReadOnlyList<List<T?>> data) where T : struct
    {
        reader.GetOrdinal(columnName).Should().Be(columnIndex);

        reader.GetFieldValue<List<T?>>(columnIndex).Should().BeEquivalentTo(data[0]);

        reader.Read();

        reader.GetFieldValue<List<T?>>(columnIndex).Should().BeEquivalentTo(data[1]);

        reader.Read();

        reader.IsDBNull(columnIndex).Should().Be(true);
    }

    private void VerifyDataListClass<T>(string columnName, int columnIndex, IReadOnlyList<List<T>> data) where T : class
    {
        reader.GetOrdinal(columnName).Should().Be(columnIndex);

        var fieldValue = reader.GetFieldValue<List<T>>(columnIndex);
        fieldValue.Should().BeEquivalentTo(data[0]);

        reader.Read();

        reader.GetFieldValue<List<T>>(columnIndex).Should().BeEquivalentTo(data[1]);

        reader.Read();

        reader.IsDBNull(columnIndex).Should().Be(true);
    }

    [Fact]
    public void ReadBool()
    {
        VerifyDataStruct<bool>("bool", 0, new List<bool> { false, true });
    }

    [Fact]
    public void ReadTinyInt()
    {
        VerifyDataStruct<sbyte>("tinyint", 1, new List<sbyte> { sbyte.MinValue, sbyte.MaxValue });
    }

    [Fact]
    public void ReadSmallInt()
    {
        VerifyDataStruct<short>("smallint", 2, new List<short> { short.MinValue, short.MaxValue });
    }

    [Fact]
    public void ReadInt()
    {
        VerifyDataStruct<int>("int", 3, new List<int> { int.MinValue, int.MaxValue });
    }

    [Fact]
    public void ReadBigInt()
    {
        VerifyDataStruct<long>("bigint", 4, new List<long> { long.MinValue, long.MaxValue });
    }

    [Fact]
    public void ReadHugeInt()
    {
        VerifyDataStruct<BigInteger>("hugeint", 5, new List<BigInteger>
        {
            BigInteger.Parse("-170141183460469231731687303715884105727"),
            BigInteger.Parse("170141183460469231731687303715884105727")
        });
    }

    [Fact]
    public void ReadUTinyInt()
    {
        VerifyDataStruct<byte>("utinyint", 6, new List<byte> { 0, byte.MaxValue });
    }

    [Fact]
    public void ReadUSmallInt()
    {
        VerifyDataStruct<ushort>("usmallint", 7, new List<ushort> { 0, ushort.MaxValue });
    }

    [Fact]
    public void ReadUInt()
    {
        VerifyDataStruct<uint>("uint", 8, new List<uint> { 0, uint.MaxValue });
    }

    [Fact]
    public void ReadUBigInt()
    {
        VerifyDataStruct<ulong>("ubigint", 9, new List<ulong> { 0, ulong.MaxValue });
    }

    [Fact]
    public void ReadDate()
    {
        VerifyDataStruct<DuckDBDateOnly>("date", 10, new List<DuckDBDateOnly>
        {
            new(-5877641, 6, 25),
            new(5881580, 7, 10)
        });
    }

    [Fact]
    public void ReadTime()
    {
        VerifyDataStruct<DuckDBTimeOnly>("time", 11, new List<DuckDBTimeOnly>
        {
            new(0,0,0),
            new(23, 59, 59,999999)
        });
    }

    [Fact(Skip = "These dates can't be expressed by DateTime")]
    public void ReadTimeStamp()
    {
        VerifyDataStruct<DuckDBTimestamp>("timestamp", 12, new List<DuckDBTimestamp>
        {
            new(new(-290308, 12, 22), new(0,0,0)),
            new(new(294247, 1, 10), new(4,0,54,775806))
        });
    }

    [Fact]
    public void ReadFloat()
    {
        VerifyDataStruct<float>("float", 18, new List<float> { float.MinValue, float.MaxValue });
    }

    [Fact]
    public void ReadDouble()
    {
        VerifyDataStruct<double>("double", 19, new List<double> { double.MinValue, double.MaxValue });
    }

    [Fact]
    public void ReadDecimal1()
    {
        VerifyDataStruct<decimal>("dec_4_1", 20, new List<decimal> { -999.9m, 999.9m });
    }

    [Fact]
    public void ReadDecimal2()
    {
        VerifyDataStruct<decimal>("dec_9_4", 21, new List<decimal> { -99999.9999m, 99999.9999m });
    }

    [Fact]
    public void ReadDecimal3()
    {
        VerifyDataStruct<decimal>("dec_18_6", 22, new List<decimal> { -999999999999.999999m, 999999999999.999999m });
    }

    [Fact]
    public void ReadDecimal4()
    {
        VerifyDataStruct<decimal>("dec38_10", 23, new List<decimal> { -9999999999999999999999999999.9999999999m, 9999999999999999999999999999.9999999999m });
    }

    [Fact]
    public void ReadGuid()
    {
        VerifyDataStruct<Guid>("uuid", 24, new List<Guid> { Guid.Parse("00000000-0000-0000-0000-000000000001"), Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff") });
    }

    [Fact]
    public void ReadString()
    {
        VerifyDataClass<string>("varchar", 26, new List<string> { "🦆🦆🦆🦆🦆🦆", "goo\0se" });
    }

    [Fact]
    public void ReadBit()
    {
        VerifyDataClass<string>("bit", 28, new List<string> { "0010001001011100010101011010111", "10101" });
    }

    [Fact]
    public void ReadSmallEnum()
    {
        VerifyDataClass<string>("small_enum", 29, new List<string> { "DUCK_DUCK_ENUM", "GOOSE" });
    }

    [Fact]
    public void ReadMediumEnum()
    {
        VerifyDataClass<string>("medium_enum", 30, new List<string> { "enum_0", "enum_299" });
    }

    [Fact]
    public void ReadLargeEnum()
    {
        VerifyDataClass<string>("large_enum", 31, new List<string> { "enum_0", "enum_69999" });
    }

    [Fact]
    public void ReadIntList()
    {
        VerifyDataList<int>("int_array", 32, new List<List<int?>> { new(), new() { 42, 999, null, null, -42 } });
    }

    [Fact]
    public void ReadDoubleList()
    {
        VerifyDataList<double>("double_array", 33, new List<List<double?>> { new(), new() { 42.0, double.NaN, double.PositiveInfinity, double.NegativeInfinity, null, -42.0 } });
    }

    [Fact]
    public void ReadDateList()
    {
        VerifyDataList<DuckDBDateOnly>("date_array", 34, new List<List<DuckDBDateOnly?>> { new(), new()
        {
            new DuckDBDateOnly(1970, 1, 1),
            new DuckDBDateOnly(5881580, 7, 11),
            new DuckDBDateOnly(-5877641, 6, 24),
            null,
            new DuckDBDateOnly(2022,5,12),
        } });
    }

    [Fact]
    public void ReadStringList()
    {
        VerifyDataListClass<string>("varchar_array", 37, new List<List<string>> { new(), new() { "🦆🦆🦆🦆🦆🦆", "goose", null, "" } });
    }

    [Fact]
    public void ReadNestedIntList()
    {
        var data = new List<int?>() { 42, 999, null, null, -42 };
        VerifyDataListClass<List<int?>>("nested_int_array", 38, new List<List<List<int?>>> {new (), new()
        {
            new(),
            data,
            null,
            new(),
            data,
        } });
    }

    [Fact]
    public void ReadStruct()
    {
        var columnIndex = 39;
        reader.GetOrdinal("struct").Should().Be(columnIndex);

        reader.GetValue(columnIndex).Should().BeEquivalentTo(new Dictionary<string, object>() { { "a", null }, { "b", null } });
        reader.GetFieldValue<StructTest>(columnIndex).Should().BeEquivalentTo(new StructTest());

        reader.Read();

        reader.GetValue(columnIndex).Should().BeEquivalentTo(new Dictionary<string, object>() { { "a", 42 }, { "b", "🦆🦆🦆🦆🦆🦆" } });
        reader.GetFieldValue<StructTest>(columnIndex).Should().BeEquivalentTo(new StructTest()
        {
            A = 42,
            B = "🦆🦆🦆🦆🦆🦆"
        });

        reader.Read();

        reader.IsDBNull(columnIndex).Should().Be(true);
    }

    [Fact]
    public void ReadStructOfArray()
    {
        var columnIndex = 40;
        reader.GetOrdinal("struct_of_arrays").Should().Be(columnIndex);

        reader.GetFieldValue<StructOfArrayTest>(columnIndex).Should().BeEquivalentTo(new StructOfArrayTest());

        reader.Read();

        reader.GetFieldValue<StructOfArrayTest>(columnIndex).Should().BeEquivalentTo(new StructOfArrayTest()
        {
            A = new() { 42, 999, null, null, -42 },
            B = new() { "🦆🦆🦆🦆🦆🦆", "goose", null, "" }
        });

        reader.Read();

        reader.IsDBNull(columnIndex).Should().Be(true);
    }

    [Fact]
    public void ReadArrayOfStructs()
    {
        var columnIndex = 41;
        reader.GetOrdinal("array_of_structs").Should().Be(columnIndex);

        reader.GetFieldValue<List<StructTest>>(columnIndex).Should().BeEquivalentTo(new List<StructTest>());

        reader.Read();

        reader.GetFieldValue<List<StructTest>>(columnIndex).Should().BeEquivalentTo(new List<StructTest>()
        {
            new(),
            new()
            {
                A = 42,
                B = "🦆🦆🦆🦆🦆🦆"
            },
            null
        });

        reader.Read();

        reader.IsDBNull(columnIndex).Should().Be(true);
    }

    [Fact]
    public void ReadMap()
    {
        var columnIndex = 42;
        reader.GetOrdinal("map").Should().Be(columnIndex);

        reader.GetValue(columnIndex).Should().BeEquivalentTo(new Dictionary<string, object>());

        reader.Read();

        reader.GetValue(columnIndex).Should().BeEquivalentTo(new Dictionary<string, object>() { { "key1", "🦆🦆🦆🦆🦆🦆" }, { "key2", "goose" } });

        reader.Read();

        reader.IsDBNull(columnIndex).Should().Be(true);
    }

    class StructTest
    {
        public int? A { get; set; }
        public string B { get; set; }
    }

    class StructOfArrayTest
    {
        public List<int?> A { get; set; }
        public List<string> B { get; set; }
    }
}