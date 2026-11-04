using System.Globalization;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Data.Mapping;

namespace DuckDB.NET.Test;

// Int128 and UInt128 written by the appender, the mapped appender and functions (2.0 item 9). An Int128 is what a
// HUGEINT holds and a UInt128 what a UHUGEINT holds; like the other integers, each also goes to the other column
// without a check, reinterpreted. Every
// value is compared with the one DuckDB parses from its text, so a mistake in the two halves that a round trip
// through this library would hide shows here.
public class HugeIntWriteConversionTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    private static readonly Int128[] SignedValues =
    [
        Int128.MinValue, Int128.MinValue + 1, (Int128)long.MinValue - 1, long.MinValue, -1, 0, 1, long.MaxValue,
        ulong.MaxValue, (Int128)ulong.MaxValue + 1, Int128.MaxValue - 1, Int128.MaxValue,
    ];

    private static readonly UInt128[] UnsignedValues =
    [
        UInt128.Zero, UInt128.One, ulong.MaxValue, (UInt128)ulong.MaxValue + 1, (UInt128)Int128.MaxValue,
        (UInt128)Int128.MaxValue + 1, UInt128.MaxValue - 1, UInt128.MaxValue,
    ];

    [Fact]
    public void AppendedInt128AndUInt128AreTheValuesDuckDBParses()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE hugeIntWriteSigned (text VARCHAR, value HUGEINT)");
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE hugeIntWriteUnsigned (text VARCHAR, value UHUGEINT)");

        using (var appender = Connection.CreateAppender("hugeIntWriteSigned"))
        {
            foreach (var value in SignedValues)
            {
                appender.CreateRow().AppendValue(value.ToString(CultureInfo.InvariantCulture)).AppendValue(value).EndRow();
            }

            appender.CreateRow().AppendValue((string)null).AppendValue((Int128?)null).EndRow();
        }

        using (var appender = Connection.CreateAppender("hugeIntWriteUnsigned"))
        {
            foreach (var value in UnsignedValues)
            {
                appender.CreateRow().AppendValue(value.ToString(CultureInfo.InvariantCulture)).AppendValue(value).EndRow();
            }

            appender.CreateRow().AppendValue((string)null).AppendValue((UInt128?)null).EndRow();
        }

        CountDifferences("hugeIntWriteSigned", "HUGEINT").Should().Be((SignedValues.Length + 1, 0));
        CountDifferences("hugeIntWriteUnsigned", "UHUGEINT").Should().Be((UnsignedValues.Length + 1, 0));
    }

    // Like the other integers, each type goes into the other 128-bit column as its bytes, without a check: an Int128 of
    // -1 in a UHUGEINT column is 2^128 - 1, as a long of -1 in a UBIGINT column is 2^64 - 1.
    [Fact]
    public void Int128AndUInt128GoToTheOtherColumnReinterpreted()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE hugeIntWriteAcross (signed HUGEINT, unsigned UHUGEINT, expected_signed VARCHAR, expected_unsigned VARCHAR)");

        using (var appender = Connection.CreateAppender("hugeIntWriteAcross"))
        {
            appender.CreateRow().AppendValue(UInt128.Zero).AppendValue(Int128.Zero).AppendValue("0").AppendValue("0").EndRow();
            appender.CreateRow().AppendValue((UInt128)Int128.MaxValue).AppendValue(Int128.MaxValue)
                .AppendValue("170141183460469231731687303715884105727").AppendValue("170141183460469231731687303715884105727").EndRow();
            appender.CreateRow().AppendValue((UInt128)Int128.MaxValue + 1).AppendValue(Int128.NegativeOne)
                .AppendValue("-170141183460469231731687303715884105728").AppendValue("340282366920938463463374607431768211455").EndRow();
            appender.CreateRow().AppendValue(UInt128.MaxValue).AppendValue(Int128.MinValue)
                .AppendValue("-1").AppendValue("170141183460469231731687303715884105728").EndRow();
        }

        Command.CommandText = """
            SELECT count(*), count(*) FILTER (WHERE signed IS DISTINCT FROM expected_signed::HUGEINT OR unsigned IS DISTINCT FROM expected_unsigned::UHUGEINT)
            FROM hugeIntWriteAcross
            """;
        using var reader = Command.ExecuteReader();
        reader.Read();
        reader.GetInt64(0).Should().Be(4);
        reader.GetInt64(1).Should().Be(0);
    }

    // The 16 bytes do not fit the slot of a narrower column, so they are refused there.
    [Fact]
    public void Int128AndUInt128AreRefusedByOtherColumns()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE hugeIntWriteOther (a BIGINT, b DOUBLE)");

        using (var appender = Connection.CreateAppender("hugeIntWriteOther"))
        {
            appender.CreateRow().Invoking(r => r.AppendValue(Int128.One)).Should().Throw<InvalidOperationException>()
                .WithMessage("Cannot write Int128 to BigInt column");
        }

        using (var appender = Connection.CreateAppender("hugeIntWriteOther"))
        {
            appender.CreateRow().AppendValue((long?)1).Invoking(r => r.AppendValue(UInt128.One)).Should().Throw<InvalidOperationException>()
                .WithMessage("Cannot write UInt128 to Double column");
        }
    }

    [Fact]
    public void ListsOfInt128AndUInt128()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE hugeIntWriteLists (signed HUGEINT[], unsigned UHUGEINT[], fixed HUGEINT[2])");

        using (var appender = Connection.CreateAppender("hugeIntWriteLists"))
        {
            appender.CreateRow()
                .AppendValue(new List<Int128> { Int128.MinValue, -1, Int128.MaxValue })
                .AppendValue(new UInt128?[] { UInt128.MaxValue, null, 0 })
                .AppendValue(new List<Int128?> { null, Int128.MaxValue })
                .EndRow();
        }

        Command.CommandText = """
            SELECT signed = ['-170141183460469231731687303715884105728', '-1', '170141183460469231731687303715884105727']::HUGEINT[],
                   unsigned IS NOT DISTINCT FROM ['340282366920938463463374607431768211455', NULL, '0']::UHUGEINT[],
                   fixed IS NOT DISTINCT FROM [NULL, '170141183460469231731687303715884105727']::HUGEINT[2]
            FROM hugeIntWriteLists
            """;
        using var reader = Command.ExecuteReader();
        reader.Read();
        reader.GetBoolean(0).Should().BeTrue();
        reader.GetBoolean(1).Should().BeTrue();
        reader.GetBoolean(2).Should().BeTrue();
    }

    [Fact]
    public void MappedInt128AndUInt128Properties()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE hugeIntWriteMapped (signed HUGEINT, unsigned UHUGEINT, nullable_signed HUGEINT, nullable_unsigned UHUGEINT)");

        using (var appender = Connection.CreateAppender<WideRecord, WideRecordMap>("hugeIntWriteMapped"))
        {
            appender.AppendRecords([
                new WideRecord { Signed = Int128.MinValue, Unsigned = UInt128.MaxValue, NullableSigned = Int128.MaxValue, NullableUnsigned = null },
                new WideRecord { Signed = -1, Unsigned = 0, NullableSigned = null, NullableUnsigned = UInt128.MaxValue },
            ]);
        }

        Command.CommandText = """
            SELECT count(*) FILTER (WHERE (signed, unsigned, nullable_signed, nullable_unsigned) IS NOT DISTINCT FROM
                       ('-170141183460469231731687303715884105728'::HUGEINT, '340282366920938463463374607431768211455'::UHUGEINT,
                        '170141183460469231731687303715884105727'::HUGEINT, NULL::UHUGEINT)),
                   count(*) FILTER (WHERE (signed, unsigned, nullable_signed, nullable_unsigned) IS NOT DISTINCT FROM
                       ((-1)::HUGEINT, 0::UHUGEINT, NULL::HUGEINT, '340282366920938463463374607431768211455'::UHUGEINT))
            FROM hugeIntWriteMapped
            """;
        using var reader = Command.ExecuteReader();
        reader.Read();
        reader.GetInt64(0).Should().Be(1);
        reader.GetInt64(1).Should().Be(1);
    }

    // The mapped appender checks types when it is created. Like a long, which it accepts for BIGINT only, each type is
    // accepted for its own column only.
    [Fact]
    public void MappedInt128AndUInt128AreRefusedForTheOtherColumn()
    {
        Connection.ExecuteNonQuery("CREATE OR REPLACE TABLE hugeIntWriteMappedAcross (signed UHUGEINT, unsigned HUGEINT)");

        Connection.Invoking(c => c.CreateAppender<CrossedRecord, SignedMap>("hugeIntWriteMappedAcross")).Should().Throw<InvalidOperationException>()
            .WithMessage("Type mismatch at column index 0: Mapped type is Int128 (expected DuckDB type: HugeInt) but actual column type is UnsignedHugeInt");

        Connection.Invoking(c => c.CreateAppender<CrossedRecord, UnsignedMap>("hugeIntWriteMappedAcross")).Should().Throw<InvalidOperationException>()
            .WithMessage("Type mismatch at column index 1: Mapped type is UInt128 (expected DuckDB type: UnsignedHugeInt) but actual column type is HugeInt");
    }

    [Fact]
    public void ScalarFunctionsTakeAndReturnInt128AndUInt128()
    {
        Connection.RegisterScalarFunction<Int128, Int128>("int128_minus_one", value => value - 1);
        Connection.RegisterScalarFunction<UInt128, UInt128?>("uint128_halved_or_null", value => value == 0 ? null : value / 2);
        Connection.RegisterScalarFunction<UInt128, string>("uint128_text", value => value.ToString(CultureInfo.InvariantCulture));

        Command.CommandText = """
            SELECT int128_minus_one('170141183460469231731687303715884105727'::HUGEINT) = '170141183460469231731687303715884105726'::HUGEINT,
                   int128_minus_one((-5)::INTEGER) = -6,
                   uint128_halved_or_null('340282366920938463463374607431768211455'::UHUGEINT) = '170141183460469231731687303715884105727'::UHUGEINT,
                   uint128_halved_or_null(0::UHUGEINT) IS NULL,
                   uint128_text('340282366920938463463374607431768211455'::UHUGEINT)
            """;
        using var reader = Command.ExecuteReader();
        reader.Read();
        reader.GetBoolean(0).Should().BeTrue();
        reader.GetBoolean(1).Should().BeTrue();
        reader.GetBoolean(2).Should().BeTrue();
        reader.GetBoolean(3).Should().BeTrue();
        reader.GetString(4).Should().Be("340282366920938463463374607431768211455");
    }

    // 3,000 rows, so the values go into two chunks.
    [Fact]
    public void ATableFunctionReturnsInt128AndUInt128Columns()
    {
        Connection.RegisterTableFunction("int128_table",
            (int count) => Enumerable.Range(0, count),
            index => new { Index = index, Signed = Int128.MaxValue - index, Unsigned = UInt128.MaxValue - (UInt128)index, NullableSigned = index % 2 == 0 ? (Int128?)null : -index });

        Command.CommandText = """
            SELECT count(*),
                   count(*) FILTER (WHERE Signed IS DISTINCT FROM '170141183460469231731687303715884105727'::HUGEINT - Index
                                       OR Unsigned IS DISTINCT FROM '340282366920938463463374607431768211455'::UHUGEINT - Index::UHUGEINT
                                       OR NullableSigned IS DISTINCT FROM CASE WHEN Index % 2 = 0 THEN NULL ELSE -Index::HUGEINT END)
            FROM int128_table(3000)
            """;
        using var reader = Command.ExecuteReader();
        reader.Read();
        reader.GetInt64(0).Should().Be(3000);
        reader.GetInt64(1).Should().Be(0);
    }

    // A parameter of a table function is a DuckDB value, read by DuckDBValue.GetValue<T>, which reinterprets what it
    // makes as T: an Int128 read as a BigInteger gave the bytes of the BigInteger.
    [Fact]
    public void ATableFunctionTakesInt128AndUInt128Parameters()
    {
        Connection.RegisterTableFunction("int128_parameters", (Int128 signed, UInt128 unsigned) => new[] { new WideParameters(signed, unsigned) },
            row => new { Signed = row.Signed.ToString(CultureInfo.InvariantCulture), Unsigned = row.Unsigned.ToString(CultureInfo.InvariantCulture) });

        Command.CommandText = "SELECT * FROM int128_parameters('-170141183460469231731687303715884105728'::HUGEINT, '340282366920938463463374607431768211455'::UHUGEINT)";
        using (var reader = Command.ExecuteReader())
        {
            reader.Read();
            reader.GetString(0).Should().Be("-170141183460469231731687303715884105728");
            reader.GetString(1).Should().Be("340282366920938463463374607431768211455");
        }

        Connection.RegisterTableFunction<Int128, UInt128>("int128_parameter_values", parameters =>
        {
            var signed = parameters[0].GetValue<Int128>();
            var unsigned = parameters[1].GetValue<UInt128>();
            var big = parameters[0].GetValue<BigInteger>();

            return new TableFunction([new ColumnInfo("text", typeof(string))],
                new[] { $"{signed.ToString(CultureInfo.InvariantCulture)} {unsigned.ToString(CultureInfo.InvariantCulture)} {big.ToString(CultureInfo.InvariantCulture)}" });
        }, (item, writers, rowIndex) => writers[0].WriteValue((string)item, rowIndex));

        Command.CommandText = "SELECT * FROM int128_parameter_values(42::HUGEINT, '170141183460469231731687303715884105728'::UHUGEINT)";
        Command.ExecuteScalar().Should().Be("42 170141183460469231731687303715884105728 42");
    }

    private (long Rows, long Different) CountDifferences(string table, string type)
    {
        Command.CommandText = $"SELECT count(*), count(*) FILTER (WHERE value IS DISTINCT FROM text::{type}) FROM {table}";
        using var reader = Command.ExecuteReader();
        reader.Read();
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    public record WideParameters(Int128 Signed, UInt128 Unsigned);

    public class WideRecord
    {
        public Int128 Signed { get; set; }
        public UInt128 Unsigned { get; set; }
        public Int128? NullableSigned { get; set; }
        public UInt128? NullableUnsigned { get; set; }
    }

    public class WideRecordMap : DuckDBAppenderMap<WideRecord>
    {
        public WideRecordMap()
        {
            Map(record => record.Signed);
            Map(record => record.Unsigned);
            Map(record => record.NullableSigned);
            Map(record => record.NullableUnsigned);
        }
    }

    public class CrossedRecord
    {
        public Int128 Signed { get; set; }
        public UInt128 Unsigned { get; set; }
    }

    // Int128 into the UHUGEINT column; the second column is left to its default.
    public class SignedMap : DuckDBAppenderMap<CrossedRecord>
    {
        public SignedMap()
        {
            Map(record => record.Signed);
            DefaultValue();
        }
    }

    // The first column is left to its default; UInt128 into the HUGEINT column.
    public class UnsignedMap : DuckDBAppenderMap<CrossedRecord>
    {
        public UnsignedMap()
        {
            DefaultValue();
            Map(record => record.Unsigned);
        }
    }
}
