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
        Command.CommandText = "Select * from test_all_types()";
        reader = Command.ExecuteReader();
        reader.Read();
    }

    private void VerifyData<T>(string columnName, int columnIndex, IReadOnlyList<T> data) where T : struct
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
        VerifyData<bool>("bool", 0, new List<bool> { false, true });
    }

    [Fact]
    public void ReadTinyInt()
    {
        VerifyData<sbyte>("tinyint", 1, new List<sbyte> { sbyte.MinValue, sbyte.MaxValue });
    }

    [Fact]
    public void ReadSmallInt()
    {
        VerifyData<short>("smallint", 2, new List<short> { short.MinValue, short.MaxValue });
    }

    [Fact]
    public void ReadInt()
    {
        VerifyData<int>("int", 3, new List<int> { int.MinValue, int.MaxValue });
    }

    [Fact]
    public void ReadBigInt()
    {
        VerifyData<long>("bigint", 4, new List<long> { long.MinValue, long.MaxValue });
    }

    [Fact]
    public void ReadHugeInt()
    {
        VerifyData<BigInteger>("hugeint", 5, new List<BigInteger>
        {
            BigInteger.Parse("-170141183460469231731687303715884105727"),
            BigInteger.Parse("170141183460469231731687303715884105727")
        });
    }

    [Fact]
    public void ReadUTinyInt()
    {
        VerifyData<byte>("utinyint", 6, new List<byte> { 0, byte.MaxValue });
    }

    [Fact]
    public void ReadUSmallInt()
    {
        VerifyData<ushort>("usmallint", 7, new List<ushort> { 0, ushort.MaxValue });
    }

    [Fact]
    public void ReadUInt()
    {
        VerifyData<uint>("uint", 8, new List<uint> { 0, uint.MaxValue });
    }

    [Fact]
    public void ReadUBigInt()
    {
        VerifyData<ulong>("ubigint", 9, new List<ulong> { 0, ulong.MaxValue });
    }

    [Fact]
    public void ReadDate()
    {
        VerifyData<DuckDBDateOnly>("date", 10, new List<DuckDBDateOnly>
        {
            new DuckDBDateOnly(-5877641, 6, 25),
            new DuckDBDateOnly(5881580, 7, 10)
        });
    }

    [Fact]
    public void ReadTime()
    {
        VerifyData<DuckDBTimeOnly>("time", 11, new List<DuckDBTimeOnly>
        {
            new DuckDBTimeOnly(0,0,0),
            new DuckDBTimeOnly(23, 59, 59,999999)
        });
    }

    [Fact(Skip = "These dates can't be expressed by DateTime")]
    public void ReadTimeStamp()
    {
        VerifyData<DuckDBTimestamp>("timestamp", 12, new List<DuckDBTimestamp>
        {
            new DuckDBTimestamp(new DuckDBDateOnly(-290308, 12, 22), new DuckDBTimeOnly(0,0,0)),
            new DuckDBTimestamp(new DuckDBDateOnly(294247, 1, 10), new DuckDBTimeOnly(4,0,54,775806))
        });
    }

    [Fact]
    public void ReadFloat()
    {
        VerifyData<float>("float", 18, new List<float> { float.MinValue, float.MaxValue });
    }

    [Fact]
    public void ReadDouble()
    {
        VerifyData<double>("double", 19, new List<double> { double.MinValue, double.MaxValue });
    }

    [Fact]
    public void ReadDecimal1()
    {
        VerifyData<decimal>("dec_4_1", 20, new List<decimal> { -999.9m, 999.9m });
    }

    [Fact]
    public void ReadDecimal2()
    {
        VerifyData<decimal>("dec_9_4", 21, new List<decimal> { -99999.9999m, 99999.9999m });
    }

    [Fact]
    public void ReadDecimal3()
    {
        VerifyData<decimal>("dec_18_6", 22, new List<decimal> { -999999999999.999999m, 999999999999.999999m });
    }

    [Fact]
    public void ReadDecimal4()
    {
        VerifyData<decimal>("dec38_10", 23, new List<decimal> { -9999999999999999999999999999.9999999999m, 9999999999999999999999999999.9999999999m });
    }

    [Fact]
    public void ReadGuid()
    {
        VerifyData<Guid>("uuid", 24, new List<Guid> { Guid.Parse("00000000-0000-0000-0000-000000000001"), Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff") });
    }
}