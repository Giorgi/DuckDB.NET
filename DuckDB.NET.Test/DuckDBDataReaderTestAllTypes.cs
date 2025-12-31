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

    private void VerifyDataStruct<T>(string columnName, int columnIndex, IReadOnlyList<T> data, Type providerSpecificType = null, bool readProviderSpecificValue = false) where T : struct
    {
        reader.GetOrdinal(columnName).Should().Be(columnIndex);
        reader.GetProviderSpecificFieldType(columnIndex).Should().Be(providerSpecificType ?? typeof(T));

        (readProviderSpecificValue ? reader.GetProviderSpecificValue(columnIndex) : reader.GetValue(columnIndex)).Should().Be(data[0]);
        reader.GetFieldValue<T>(columnIndex).Should().Be(data[0]);

        reader.Read();

        (readProviderSpecificValue ? reader.GetProviderSpecificValue(columnIndex) : reader.GetValue(columnIndex)).Should().Be(data[1]);
        reader.GetFieldValue<T>(columnIndex).Should().Be(data[1]);

        reader.Read();

        reader.IsDBNull(columnIndex).Should().Be(true);

        reader.GetValue(columnIndex).Should().Be(DBNull.Value);
        reader.GetFieldValue<T?>(columnIndex).Should().Be(null);
        reader.GetProviderSpecificValue(columnIndex).Should().Be(DBNull.Value);

        reader.Invoking(r => r.GetFieldValue<T>(columnIndex)).Should().Throw<InvalidCastException>();
    }

    private void VerifyDataClass<T>(string columnName, int columnIndex, IReadOnlyList<T> data) where T : class
    {
        reader.GetOrdinal(columnName).Should().Be(columnIndex);
        reader.GetProviderSpecificFieldType(columnIndex).Should().Be(typeof(T));

        reader.GetValue(columnIndex).Should().Be(data[0]);
        reader.GetFieldValue<T>(columnIndex).Should().Be(data[0]);

        reader.Read();

        reader.GetValue(columnIndex).Should().Be(data[1]);
        reader.GetFieldValue<T>(columnIndex).Should().Be(data[1]);

        reader.Read();

        reader.IsDBNull(columnIndex).Should().Be(true);
        reader.Invoking(r => r.GetFieldValue<T>(columnIndex)).Should().Throw<InvalidCastException>();
    }

    private void VerifyDataList<T>(string columnName, int columnIndex, IReadOnlyList<List<T?>> data) where T : struct
    {
        reader.GetOrdinal(columnName).Should().Be(columnIndex);
        reader.GetProviderSpecificFieldType(columnIndex).Should().Be(typeof(List<T>));

        reader.GetFieldValue<List<T?>>(columnIndex).Should().BeEquivalentTo(data[0]);

        reader.Read();

        reader.GetFieldValue<List<T?>>(columnIndex).Should().BeEquivalentTo(data[1]);

        reader.Read();

        reader.IsDBNull(columnIndex).Should().Be(true);
        reader.Invoking(r => r.GetFieldValue<List<T>>(columnIndex)).Should().Throw<InvalidCastException>();
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
        reader.Invoking(r => r.GetFieldValue<T>(columnIndex)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void ReadBool()
    {
        VerifyDataStruct("bool", 0, new List<bool> { false, true });
    }

    [Fact]
    public void ReadTinyInt()
    {
        VerifyDataStruct("tinyint", 1, new List<sbyte> { sbyte.MinValue, sbyte.MaxValue });
    }

    [Fact]
    public void ReadSmallInt()
    {
        VerifyDataStruct("smallint", 2, new List<short> { short.MinValue, short.MaxValue });
    }

    [Fact]
    public void ReadInt()
    {
        VerifyDataStruct("int", 3, new List<int> { int.MinValue, int.MaxValue });
    }

    [Fact]
    public void ReadBigInt()
    {
        VerifyDataStruct("bigint", 4, new List<long> { long.MinValue, long.MaxValue });
    }

    [Fact]
    public void ReadHugeInt()
    {
        VerifyDataStruct("hugeint", 5, new List<BigInteger>
        {
            BigInteger.Parse("-170141183460469231731687303715884105728"),
            BigInteger.Parse("170141183460469231731687303715884105727")
        }, typeof(DuckDBHugeInt));
    }

    [Fact]
    public void ReadUHugeInt()
    {
        VerifyDataStruct("uhugeint", 6, new List<BigInteger>
        {
            BigInteger.Zero,
            BigInteger.Parse("340282366920938463463374607431768211455")
        }, typeof(DuckDBUHugeInt));
    }

    [Fact]
    public void ReadUTinyInt()
    {
        VerifyDataStruct("utinyint", 7, new List<byte> { 0, byte.MaxValue });
    }

    [Fact]
    public void ReadUSmallInt()
    {
        VerifyDataStruct("usmallint", 8, new List<ushort> { 0, ushort.MaxValue });
    }

    [Fact]
    public void ReadUInt()
    {
        VerifyDataStruct("uint", 9, new List<uint> { 0, uint.MaxValue });
    }

    [Fact]
    public void ReadUBigInt()
    {
        VerifyDataStruct("ubigint", 10, new List<ulong> { 0, ulong.MaxValue });
    }

    [Fact]
    public void ReadVarint()
    {
        VerifyDataStruct("bignum", 11, new List<BigInteger>
        {
            BigInteger.Parse("-179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368"),
            BigInteger.Parse("179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368"),
        });
    }

    [Fact]
    public void ReadDate()
    {
        VerifyDataStruct("date", 12, new List<DuckDBDateOnly>
        {
            new(-5877641, 6, 25),
            new(5881580, 7, 10)
        }, typeof(DuckDBDateOnly), true);
    }

    [Fact]
    public void ReadTime()
    {
        VerifyDataStruct("time", 13, new List<DuckDBTimeOnly>
        {
            new(0,0,0),
            new(24, 0, 0,0)
        }, typeof(DuckDBTimeOnly), true);
    }

    [Fact]
    public void ReadTimeStamp()
    {
        VerifyDataStruct("timestamp", 14, new List<DuckDBTimestamp>
        {
            new(new(-290308, 12, 22), new(0,0,0)),
            new(new(294247, 1, 10), new(4,0,54,775806))
        }, typeof(DuckDBTimestamp), true);
    }

    [Fact]
    public void ReadTimeStampS()
    {
        VerifyDataStruct("timestamp_s", 15, new List<DuckDBTimestamp>
        {
            new(new(-290308, 12, 22), new(0,0,0)),
            new(new(294247, 1, 10), new(4,0,54))
        }, readProviderSpecificValue: true);
    }

    [Fact]
    public void ReadTimeStampMS()
    {
        VerifyDataStruct("timestamp_ms", 16, new List<DuckDBTimestamp>
        {
            new(new(-290308, 12, 22), new(0,0,0)),
            new(new(294247, 1, 10), new(4,0,54,775000))
        }, readProviderSpecificValue: true);
    }

    [Fact]
    public void ReadTimeStampNS()
    {
        VerifyDataStruct("timestamp_ns", 17, new List<DateTime>
        {
            new DateTime(1677, 09, 22),
            new DateTime (2262, 04, 11, 23,47,16).AddTicks(8547758)
        }, typeof(DuckDBTimestamp));
    }

    [Fact]
    public void ReadTimeTz()
    {
        VerifyDataStruct("time_tz", 18, new List<DuckDBTimeTz>
        {
            new()
            {
                Offset = (int)new TimeSpan(15,59,59).TotalSeconds,
                Time = new()
            },
            new()
            {
                Offset = -(int)new TimeSpan(15,59,59).TotalSeconds,
                Time = new(24,0,0)
            },
        }, readProviderSpecificValue: true);
    }

    [Fact]
    public void ReadTimeStampTz()
    {
        VerifyDataStruct("timestamp_tz", 19, new List<DuckDBTimestamp>
        {
            new(new(-290308, 12, 22), new(0,0,0)),
            new(new(294247, 1, 10), new(4,0,54,775806))
        }, typeof(DuckDBTimestamp), true);
    }

    [Fact]
    public void ReadFloat()
    {
        VerifyDataStruct("float", 20, new List<float> { float.MinValue, float.MaxValue });
    }

    [Fact]
    public void ReadDouble()
    {
        VerifyDataStruct("double", 21, new List<double> { double.MinValue, double.MaxValue });
    }

    [Fact]
    public void ReadDecimal1()
    {
        VerifyDataStruct("dec_4_1", 22, new List<decimal> { -999.9m, 999.9m });
    }

    [Fact]
    public void ReadDecimal2()
    {
        VerifyDataStruct("dec_9_4", 23, new List<decimal> { -99999.9999m, 99999.9999m });
    }

    [Fact]
    public void ReadDecimal3()
    {
        VerifyDataStruct("dec_18_6", 24, new List<decimal> { -999999999999.999999m, 999999999999.999999m });
    }

    [Fact]
    public void ReadDecimal4()
    {
        VerifyDataStruct("dec38_10", 25, new List<decimal> { -9999999999999999999999999999.9999999999m, 9999999999999999999999999999.9999999999m });
    }

    [Fact]
    public void ReadGuid()
    {
        VerifyDataStruct("uuid", 26, new List<Guid> { Guid.Parse("00000000-0000-0000-0000-000000000000"), Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff") });
    }

    [Fact]
    public void ReadInterval()
    {
        VerifyDataStruct("interval", 27, new List<DuckDBInterval>
        {
            new(),
            new(999,999,999999999),
        }, typeof(DuckDBInterval), true);
    }

    [Fact]
    public void ReadString()
    {
        VerifyDataClass("varchar", 28, new List<string> { "🦆🦆🦆🦆🦆🦆", "goo\0se" });
    }

    [Fact]
    public void ReadBlob()
    {
        var columnIndex = 29;
        reader.GetOrdinal("blob").Should().Be(columnIndex);
        reader.GetProviderSpecificFieldType(columnIndex).Should().Be(typeof(Stream));

        using (var stream = reader.GetStream(columnIndex))
        {
            var streamReader = new StreamReader(stream);

            streamReader.ReadToEnd().Should().Be("thisisalongblob\x00withnullbytes");
        }

        reader.Read();

        using (var stream = reader.GetStream(columnIndex))
        {
            var streamReader = new StreamReader(stream);

            streamReader.ReadToEnd().Should().Be(new(new char[] { (char)0, (char)0, (char)0, (char)97 }));
        }

        reader.Read();

        reader.IsDBNull(columnIndex).Should().Be(true);
        reader.Invoking(r => r.GetStream(columnIndex)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void ReadBit()
    {
        VerifyDataClass("bit", 30, new List<string> { "0010001001011100010101011010111", "10101" });
    }

    [Fact]
    public void ReadSmallEnum()
    {
        VerifyDataClass("small_enum", 31, new List<string> { "DUCK_DUCK_ENUM", "GOOSE" });
    }

    [Fact]
    public void ReadMediumEnum()
    {
        VerifyDataClass("medium_enum", 32, new List<string> { "enum_0", "enum_299" });
    }

    [Fact]
    public void ReadLargeEnum()
    {
        VerifyDataClass("large_enum", 33, new List<string> { "enum_0", "enum_69999" });
    }

    [Fact]
    public void ReadIntList()
    {
        VerifyDataList("int_array", 34, new List<List<int?>> { new(), new() { 42, 999, null, null, -42 } });
    }

    [Fact]
    public void ReadDoubleList()
    {
        VerifyDataList("double_array", 35, new List<List<double?>> { new(), new() { 42.0, double.NaN, double.PositiveInfinity, double.NegativeInfinity, null, -42.0 } });
    }

    [Fact]
    public void ReadDateList()
    {
        VerifyDataList("date_array", 36, new List<List<DuckDBDateOnly?>> { new(), new()
        {
            new DuckDBDateOnly(1970, 1, 1),
            DuckDBDateOnly.PositiveInfinity,
            DuckDBDateOnly.NegativeInfinity,
            null,
            new DuckDBDateOnly(2022,5,12),
        } });
    }

    [Fact()]
    public void ReadTimeStampList()
    {
        VerifyDataList("timestamp_array", 37, new List<List<DuckDBTimestamp?>> { new(), new()
        {
            new DuckDBTimestamp(new DuckDBDateOnly(1970,1,1), new DuckDBTimeOnly(0,0,0)),
            DuckDBTimestamp.PositiveInfinity,
            DuckDBTimestamp.NegativeInfinity,
            null,
            new DuckDBTimestamp(new DuckDBDateOnly(2022,5,12), new DuckDBTimeOnly(16,23,45))
        } });
    }

    [Fact()]
    public void ReadTimeStampTZList()
    {
        VerifyDataList("timestamptz_array", 38, new List<List<DuckDBTimestamp?>> { new(), new()
        {
            new DuckDBTimestamp(new DuckDBDateOnly(1970,1,1), new DuckDBTimeOnly(0,0,0)),
            DuckDBTimestamp.PositiveInfinity,
            DuckDBTimestamp.NegativeInfinity,
            null,
            new DuckDBTimestamp(new DuckDBDateOnly(2022,5,12), new DuckDBTimeOnly(23,23,45))
        } });
    }

    [Fact]
    public void ReadStringList()
    {
        VerifyDataListClass("varchar_array", 39, new List<List<string>> { new(), new() { "🦆🦆🦆🦆🦆🦆", "goose", null, "" } });
    }

    [Fact]
    public void ReadNestedIntList()
    {
        var data = new List<int?>() { 42, 999, null, null, -42 };
        VerifyDataListClass("nested_int_array", 40, new List<List<List<int?>>> {new (), new()
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
        var columnIndex = 41;
        reader.GetOrdinal("struct").Should().Be(columnIndex);
        reader.GetProviderSpecificFieldType(columnIndex).Should().Be(typeof(Dictionary<string, object>));

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

        reader.Invoking(r => r.GetFieldValue<StructTest>(columnIndex)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void ReadStructOfArray()
    {
        var columnIndex = 42;
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
        reader.Invoking(r => r.GetFieldValue<StructOfArrayTest>(columnIndex)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void ReadArrayOfStructs()
    {
        var columnIndex = 43;
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
        reader.Invoking(r => r.GetFieldValue<List<StructTest>>(columnIndex)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void ReadMap()
    {
        var columnIndex = 44;
        reader.GetOrdinal("map").Should().Be(columnIndex);

        reader.GetValue(columnIndex).Should().BeEquivalentTo(new Dictionary<string, string>());
        reader.GetFieldValue<Dictionary<string, string>>(columnIndex).Should().BeEquivalentTo(new Dictionary<string, string>());

        reader.Read();

        reader.GetValue(columnIndex).Should().BeEquivalentTo(new Dictionary<string, string>() { { "key1", "🦆🦆🦆🦆🦆🦆" }, { "key2", "goose" } });
        reader.GetFieldValue<Dictionary<string, string>>(columnIndex).Should().BeEquivalentTo(new Dictionary<string, string>() { { "key1", "🦆🦆🦆🦆🦆🦆" }, { "key2", "goose" } });

        reader.Read();

        reader.IsDBNull(columnIndex).Should().Be(true);
        reader.Invoking(r => r.GetFieldValue<Dictionary<string, string>>(columnIndex)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void ReadFixedIntArray()
    {
        VerifyDataList("fixed_int_array", 46, new List<List<int?>> { new() { null, 2, 3 }, new() { 4, 5, 6 }, new() { 42, 999, null, null, -42 } });
    }

    [Fact]
    public void ReadFixedVarcharArray()
    {
        VerifyDataListClass("fixed_varchar_array", 47, new List<List<string>> { new() { "a", null, "c" }, new() { "d", "e", "f" } });
    }

    [Fact]
    public void ReadFixedNestedIntArray()
    {
        VerifyDataListClass("fixed_nested_int_array", 48, new List<List<List<int?>>>
        {
            new ()
            {
                new() { null, 2, 3},
                null,
                new() { null, 2, 3}
            },
            new()
            {
                new() { 4, 5, 6 },
                new() { null, 2,3},
                new() { 4, 5, 6 },
            }
        });
    }

    [Fact]
    public void ReadFixedNestedVarcharArray()
    {
        VerifyDataListClass("fixed_nested_varchar_array", 49, new List<List<List<string>>>
        {
            new ()
            {
                new() {  "a", null, "c" },
                null,
                new() {  "a", null, "c" }
            },
            new()
            {
                new() { "d", "e", "f" },
                new() { "a", null, "c" },
                new() { "d", "e", "f" },
            }
        });
    }

    [Fact]
    public void ReadFixedStructArray()
    {
        var columnIndex = 50;
        reader.GetOrdinal("fixed_struct_array").Should().Be(columnIndex);

        reader.GetFieldValue<List<StructTest>>(columnIndex).Should().BeEquivalentTo(new List<StructTest>()
        {
            new(),
            new()
            {
                A = 42,
                B = "🦆🦆🦆🦆🦆🦆"
            },
            new()
        });

        reader.Read();

        reader.GetFieldValue<List<StructTest>>(columnIndex).Should().BeEquivalentTo(new List<StructTest>()
        {
            new()
            {
                A = 42,
                B = "🦆🦆🦆🦆🦆🦆"
            },
            new(),
            new()
            {
                A = 42,
                B = "🦆🦆🦆🦆🦆🦆"
            }
        });

        reader.Read();

        reader.IsDBNull(columnIndex).Should().Be(true);
        reader.Invoking(r => r.GetFieldValue<List<StructTest>>(columnIndex)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void ReadStructOfFixedArray()
    {
        var columnIndex = 51;
        reader.GetOrdinal("struct_of_fixed_array").Should().Be(columnIndex);

        reader.GetFieldValue<StructOfArrayTest>(columnIndex).Should().BeEquivalentTo(new StructOfArrayTest()
        {
            A = new() { null, 2, 3 },
            B = new() { "a", null, "c" }
        });

        reader.Read();

        reader.GetFieldValue<StructOfArrayTest>(columnIndex).Should().BeEquivalentTo(new StructOfArrayTest()
        {
            A = new() { 4, 5, 6 },
            B = new() { "d", "e", "f" }
        });

        reader.Read();

        reader.IsDBNull(columnIndex).Should().Be(true);
        reader.Invoking(r => r.GetFieldValue<StructOfArrayTest>(columnIndex)).Should().Throw<InvalidCastException>();
    }

    [Fact]
    public void ReadFixedArrayIntList()
    {
        VerifyDataListClass("fixed_array_of_int_list", 52, new List<List<List<int?>>>
        {
            new ()
            {
                new(),
                new() { 42, 999, null, null, -42},
                new ()
            },
            new()
            {
                new() { 42, 999, null, null, -42},
                new (),
                new() { 42, 999, null, null, -42}
            }
        });
    }

    [Fact]
    public void ReadListFixedIntArray()
    {
        VerifyDataListClass("list_of_fixed_int_array", 53, new List<List<List<int?>>>
        {
            new ()
            {
                new () {null, 2, 3},
                new() { 4, 5, 6},
                new () {null, 2, 3},
            },
            new()
            {
                new() { 4, 5, 6},
                new () {null, 2, 3},
                new() { 4, 5, 6}
            }
        });
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
