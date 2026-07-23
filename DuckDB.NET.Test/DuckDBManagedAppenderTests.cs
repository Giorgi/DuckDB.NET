using System.Globalization;
using DuckDB.NET.Data.Common;
using FluentAssertions.Common;

namespace DuckDB.NET.Test;

public class DuckDBManagedAppenderTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void CommonTypes()
    {
        var table = "CREATE TABLE managedAppenderTest(a BOOLEAN, b TINYINT, c SMALLINT, d INTEGER, e BIGINT, f UTINYINT, " +
                          "g USMALLINT, h UINTEGER, i UBIGINT, j REAL, k DOUBLE, l VARCHAR, m TIMESTAMP, n Date, o HugeInt, p UHugeInt, q decimal(9, 4));";
        Command.CommandText = table;
        Command.ExecuteNonQuery();

        var rows = 10;
        var date = DateTime.Today;
        using (var appender = Connection.CreateAppender("managedAppenderTest"))
        {
            for (var i = 0; i < rows; i++)
            {
                var row = appender.CreateRow();
                row
                    .AppendValue(i % 2 == 0)
                    .AppendValue((sbyte?)i)
                    .AppendValue((short?)i)
                    .AppendValue((int?)i)
                    .AppendValue((long?)i)
                    .AppendValue((byte?)i)
                    .AppendValue((ushort?)i)
                    .AppendValue((uint?)i)
                    .AppendValue((ulong?)i)
                    .AppendValue((float)i)
                    .AppendValue((double)i)
                    .AppendValue($"{i}")
                    .AppendValue(date.AddDays(i))
                    .AppendNullValue()
                    .AppendValue(new BigInteger(ulong.MaxValue) + i)
                    .AppendValue(new BigInteger(ulong.MaxValue) * 2 + i)
                    .AppendValue(i + i / 100m)
                    .EndRow();
            }
        }

        Command.CommandText = "SELECT * FROM managedAppenderTest";
        using (var reader = Command.ExecuteReader())
        {
            var readRowIndex = 0;
            while (reader.Read())
            {
                var booleanCell = (bool)reader[0];
                var dateTimeCell = (DateTime)reader[12];

                booleanCell.Should().Be(readRowIndex % 2 == 0);
                dateTimeCell.Should().Be(date.AddDays(readRowIndex));

                for (int columnIndex = 1; columnIndex < 12; columnIndex++)
                {
                    var cell = (IConvertible)reader[columnIndex];
                    cell.ToInt32(CultureInfo.InvariantCulture).Should().Be(readRowIndex);
                }

                reader.IsDBNull(13).Should().BeTrue();
                reader.GetFieldValue<BigInteger>(14).Should().Be(new BigInteger(ulong.MaxValue) + readRowIndex);
                reader.GetFieldValue<BigInteger>(15).Should().Be(new BigInteger(ulong.MaxValue) * 2 + readRowIndex);
                reader.GetDecimal(16).Should().Be(readRowIndex + readRowIndex / 100m);

                readRowIndex++;
            }
            readRowIndex.Should().Be(rows);
        }
    }

    [Fact]
    public void UnicodeTests()
    {
        var words = new List<string> { "hello", "안녕하세요", "Ø3mm CHAIN", null, "" };
        Command.CommandText = "CREATE TABLE UnicodeAppenderTestTable (index INTEGER, words VARCHAR);";
        Command.ExecuteNonQuery();

        using (var appender = Connection.CreateAppender("UnicodeAppenderTestTable"))
        {
            for (int i = 0; i < words.Count; i++)
            {
                var row = appender.CreateRow();
                row.AppendValue(i).AppendValue(words[i]);

                row.EndRow();
            }

            appender.Close();
        }

        Command.CommandText = "SELECT * FROM UnicodeAppenderTestTable";
        using (var reader = Command.ExecuteReader())
        {
            var results = new List<string>();
            while (reader.Read())
            {
                var text = reader.IsDBNull(1) ? null : reader.GetString(1);
                results.Add(text);
            }

            results.Should().BeEquivalentTo(words);
        }
    }

    [Fact]
    public void ByteArray()
    {
        Command.CommandText = "CREATE TABLE blobAppenderTest(a Integer, b blob)";
        Command.ExecuteNonQuery();

        var bytes = new byte[10];
        var bytes2 = new byte[10];
        Random.Shared.NextBytes(bytes);

        var span = new Span<byte>(bytes2);
        Random.Shared.NextBytes(span);

        using (var appender = Connection.CreateAppender("blobAppenderTest"))
        {
            appender.CreateRow().AppendValue(1).AppendValue(bytes).EndRow();
            appender.CreateRow().AppendValue(10).AppendValue(span).EndRow();
            appender.CreateRow().AppendValue(2).AppendValue((byte[])null).EndRow();
        }

        Command.CommandText = "Select b from blobAppenderTest";
        using var dataReader = Command.ExecuteReader();
        dataReader.Read();

        var stream = dataReader.GetStream(0);
        var memoryStream = new MemoryStream();
        stream.CopyTo(memoryStream);
        memoryStream.ToArray().Should().BeEquivalentTo(bytes);

        dataReader.Read();

        stream = dataReader.GetStream(0);
        memoryStream = new MemoryStream();
        stream.CopyTo(memoryStream);
        memoryStream.ToArray().Should().BeEquivalentTo(bytes2);

        dataReader.Read();
        dataReader.IsDBNull(0).Should().BeTrue();
    }

    [Fact]
    public void Decimals()
    {
        Command.CommandText = "CREATE TABLE managedAppenderDecimals(a INTEGER, b decimal(3, 1), c decimal (9, 4), d decimal (18, 6), e decimal(38, 12));";
        Command.ExecuteNonQuery();

        var rows = 20;
        using (var appender = Connection.CreateAppender("managedAppenderDecimals"))
        {
            for (int i = 0; i < rows; i++)
            {
                appender.CreateRow()
                    .AppendValue(i)
                    .AppendValue(i * (i % 2 == 0 ? 1m : -1m) + i / 10m)
                    .AppendValue(i * (i % 2 == 0 ? 1m : -1m) + i / 1000m)
                    .AppendValue(i * (i % 2 == 0 ? 1m : -1m) + i / 100000m)
                    .AppendValue(i * (i % 2 == 0 ? 10000000000m : -10000000000m) + i / 100000000000m)
                    .EndRow();
            }
        }

        Command.CommandText = "SELECT * FROM managedAppenderDecimals";
        using (var reader = Command.ExecuteReader())
        {
            var i = 0;
            while (reader.Read())
            {
                reader.GetDecimal(1).Should().Be(i * (i % 2 == 0 ? 1m : -1m) + i / 10m);
                reader.GetDecimal(2).Should().Be(i * (i % 2 == 0 ? 1m : -1m) + i / 1000m);
                reader.GetDecimal(3).Should().Be(i * (i % 2 == 0 ? 1m : -1m) + i / 100000m);
                reader.GetDecimal(4).Should().Be(i * (i % 2 == 0 ? 10000000000m : -10000000000m) + i / 100000000000m);
                i++;
            }
        }
    }

    [Fact]
    public void DecimalsTruncateExcessScale()
    {
        // Writing a .NET decimal with more fractional digits than the column's scale
        // should truncate the extra digits. Tests all internal storage types.
        TruncationTests("DECIMAL(4, 1)", // SmallInt internal type (width ≤ 4)
        [
            (1.19m, 1.1m),
            (-3.75m, -3.7m),
            (0.999m, 0.9m),
        ]);

        TruncationTests("DECIMAL(9, 2)", // Integer internal type (width 5-9)
        [
            (1.123m, 1.12m),
            (-999.987m, -999.98m),
            (0.005m, 0.00m),
        ]);

        TruncationTests("DECIMAL(18, 3)", // BigInt internal type (width 10-18)
        [
            (1.12345m, 1.123m),
            (-99999.99999m, -99999.999m),
            (0.0001m, 0.000m),
        ]);

        TruncationTests("DECIMAL(38, 2)", // HugeInt internal type (width > 18)
        [
            (1.123m, 1.12m),
            (-1.987m, -1.98m),
            (0.999m, 0.99m),
            (0.005m, 0.00m),
            (123456789012345678.009m, 123456789012345678.00m),
        ]);

        void TruncationTests(string columnType, (decimal input, decimal expected)[] testCases)
        {
            Command.CommandText = $"CREATE TABLE truncTest(value {columnType})";
            Command.ExecuteNonQuery();

            using (var appender = Connection.CreateAppender("truncTest"))
            {
                foreach (var (input, _) in testCases)
                {
                    appender.CreateRow().AppendValue(input).EndRow();
                }
            }

            Command.CommandText = "SELECT value FROM truncTest ORDER BY rowid";
            using (var reader = Command.ExecuteReader())
            {
                foreach (var (input, expected) in testCases)
                {
                    reader.Read().Should().BeTrue();
                    reader.GetDecimal(0).Should().Be(expected, $"for input {input} in {columnType}");
                }
            }

            Command.CommandText = "DROP TABLE truncTest";
            Command.ExecuteNonQuery();
        }
    }

    [Fact]
    public void HighScaleDecimals()
    {
        // Scale 30 exceeds .NET decimal's max scale (28), exercising the BigInteger rescaling
        // path in DecimalVectorDataWriter. Before the fix, this would crash with IndexOutOfRangeException.
        Command.CommandText = "CREATE TABLE managedAppenderHighScaleDecimals(value DECIMAL(38, 30))";
        Command.ExecuteNonQuery();

        decimal[] values = [1.5m, -1.5m, 0m, 123.456m, 0.000000001m];

        using (var appender = Connection.CreateAppender("managedAppenderHighScaleDecimals"))
        {
            foreach (var value in values)
            {
                appender.CreateRow().AppendValue(value).EndRow();
            }
        }

        Command.CommandText = "SELECT value FROM managedAppenderHighScaleDecimals ORDER BY rowid";
        using (var reader = Command.ExecuteReader())
        {
            foreach (var expected in values)
            {
                reader.Read().Should().BeTrue();
                reader.GetDecimal(0).Should().Be(expected);
            }
        }
    }

    [Fact]
    public void GuidValues()
    {
        Command.CommandText = "CREATE TABLE managedAppenderGuids(a UUID);";
        Command.ExecuteNonQuery();

        var guids = GetRandomList<Guid?>(faker => faker.Random.Guid(), 5000);
        guids.Add(null);

        using (var appender = Connection.CreateAppender("managedAppenderGuids"))
        {
            foreach (var guid in guids)
            {
                appender.CreateRow().AppendValue(guid).EndRow();
            }
        }

        var result = Connection.Query<Guid?>("SELECT * FROM managedAppenderGuids");
        result.Should().BeEquivalentTo(guids);
    }

    [Fact]
    public void IntervalValues()
    {
        Command.CommandText = "CREATE TABLE managedAppenderInterval(a INTERVAL);";
        Command.ExecuteNonQuery();

        //DuckDB's precision for Interval is MicroSeconds so results will be rounded down to the nearest 10th.
        var timeSpans = GetRandomList(faker =>
        {
            var timespan = faker.Date.Timespan();

            return TimeSpan.FromTicks(timespan.Ticks - timespan.Ticks % 10);
        });

        using (var appender = Connection.CreateAppender("managedAppenderInterval"))
        {
            foreach (var timeSpan in timeSpans)
            {
                appender.CreateRow().AppendValue(timeSpan).EndRow();
            }
        }

        var result = Connection.Query<TimeSpan>("SELECT * FROM managedAppenderInterval");

        result.Should().BeEquivalentTo(timeSpans);
    }

    [Fact]
    public void TemporalValues()
    {
        Command.CommandText = "CREATE TABLE managedAppenderTemporal(a Date, b TimeStamp, c TIMESTAMP_NS, d TIMESTAMP_MS, e TIMESTAMP_S, f TIMESTAMPTZ, g TIMETZ, h Time, i TIMESTAMPTZ);";
        Command.ExecuteNonQuery();

        var dates = Enumerable.Range(0, 20).Select(i => new DateTime(1900, 1, 1).AddDays(Random.Shared.Next(1, 50000))
                                                        .AddSeconds(Random.Shared.Next(3600 * 2, 3600 * 24))).ToList();

        using (var appender = Connection.CreateAppender("managedAppenderTemporal"))
        {
            foreach (var value in dates)
            {
                appender.CreateRow()
                                .AppendValue((DateOnly?)DateOnly.FromDateTime(value))
                                .AppendValue(value).AppendValue(value.AddTicks(1))
                                .AppendValue(value).AppendValue(value).AppendValue(value)
                                .AppendValue(value.ToDateTimeOffset(TimeSpan.FromHours(1)))
                                .AppendValue((TimeOnly?)TimeOnly.FromDateTime(value))
                                .AppendValue(new DateTimeOffset(value, TimeSpan.Zero))
                        .EndRow();
            }
        }

        var result = Connection.Query<(DateOnly, DateTime, DateTime nanos, DateTime, DateTime, DateTime, DateTimeOffset, TimeOnly, DateTimeOffset)>("SELECT a, b, c, d, e, f, g, h, i FROM managedAppenderTemporal").ToList();

        result.Select(tuple => tuple.Item1).Should().BeEquivalentTo(dates.Select(DateOnly.FromDateTime));
        result.Select(tuple => tuple.Item2).Should().BeEquivalentTo(dates);
        result.Select(tuple => tuple.nanos).Should().BeEquivalentTo(dates.Select(time => time.AddTicks(1)));
        result.Select(tuple => tuple.Item4).Should().BeEquivalentTo(dates);
        result.Select(tuple => tuple.Item5).Should().BeEquivalentTo(dates);
        result.Select(tuple => tuple.Item6).Should().BeEquivalentTo(dates);
        result.Select(tuple => tuple.Item7).Should().BeEquivalentTo(dates.Select(time => time.ToDateTimeOffset(TimeSpan.FromHours(1))));
        result.Select(tuple => tuple.Item8).Should().BeEquivalentTo(dates.Select(TimeOnly.FromDateTime));

        Command.CommandText = "Select i from managedAppenderTemporal";
        var reader = Command.ExecuteReader();

        int index = -1;
        while (reader.Read())
        {
            index++;
            reader.GetFieldValue<DateTimeOffset>(0).Should().Be(new DateTimeOffset(dates[index], TimeSpan.Zero));
        }
    }

    [Fact]
    public void EnumValues()
    {
        Command.CommandText = GetCreateEnumTypeSql("test_enum1", "test", 3);
        Command.ExecuteNonQuery();

        Command.CommandText = GetCreateEnumTypeSql("test_enum2", "test", 1000);
        Command.ExecuteNonQuery();

        Command.CommandText = GetCreateEnumTypeSql("test_enum3", "test", 100000);
        Command.ExecuteNonQuery();

        Command.CommandText = "CREATE TABLE managedAppenderEnum(a test_enum1, b test_enum1, c test_enum1, d test_enum1, e test_enum1, f test_enum2, g test_enum2, h test_enum3, i test_enum3);";
        Command.ExecuteNonQuery();

        using (var appender = Connection.CreateAppender("managedAppenderEnum"))
        {
            appender
                .CreateRow()
                .AppendNullValue()
                .AppendNullValue()
                .AppendValue("test1")
                .AppendValue(TestEnum1.Test2)
                .AppendValue(TestEnum1.Test3)
                .AppendValue("test327")
                .AppendValue(TestEnum2.Test1000)
                .AppendValue("test100000")
                .AppendValue(TestEnum3.Test6699)
                .EndRow();
        }

        var queryResult = Connection.Query<(string, TestEnum1?, TestEnum1, string, TestEnum1, TestEnum2, string, string, TestEnum3)>("SELECT a, b, c, d, e, f, g, h, i FROM managedAppenderEnum").ToList();
        var result = queryResult[0];
        result.Item1.Should().BeNull();
        result.Item2.Should().BeNull();
        result.Item3.Should().Be(TestEnum1.Test1);
        result.Item4.Should().Be("test2");
        result.Item5.Should().Be(TestEnum1.Test3);
        result.Item6.Should().Be(TestEnum2.Test327);
        result.Item7.Should().Be("test1000");
        result.Item8.Should().Be("test100000");
        result.Item9.Should().Be(TestEnum3.Test6699);
    }

    [Fact]
    public void IncompleteRowThrowsException()
    {
        var table = "CREATE TABLE managedAppenderIncompleteTest(a BOOLEAN, b TINYINT, c INTEGER);";
        Command.CommandText = table;
        Command.ExecuteNonQuery();

        Connection.Invoking(dbConnection =>
        {
            using var appender = dbConnection.CreateAppender("managedAppenderIncompleteTest");
            var row = appender.CreateRow();
            row
                .AppendValue(true)
                .AppendValue((byte)1)
                .EndRow();
        }).Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void TableDoesNotExistsThrowsException()
    {
        Connection.Invoking(dbConnection =>
        {
            using var appender = dbConnection.CreateAppender("managedAppenderMissingTableTest");
            var row = appender.CreateRow();
            row
                .AppendValue(true)
                .AppendValue((byte)1)
                .EndRow();
        }).Should().Throw<DuckDBException>();
    }

    [Fact]
    public void TooManyAppendValueThrowsException()
    {
        var table = "CREATE TABLE managedAppenderManyValuesTest(a BOOLEAN, b TINYINT);";
        Command.CommandText = table;
        Command.ExecuteNonQuery();

        Connection.Invoking(dbConnection =>
        {
            using var appender = dbConnection.CreateAppender("managedAppenderManyValuesTest");
            var row = appender.CreateRow();
            row
                .AppendValue(true)
                .AppendValue((byte)1)
                .AppendValue("test")
                .EndRow();

        }).Should().Throw<IndexOutOfRangeException>();
    }

    [Fact]
    public void WrongTypesThrowException()
    {
        var table = "CREATE TABLE managedAppenderWrongTypeTest(a BOOLEAN, c Date, b TINYINT);";
        Command.CommandText = table;
        Command.ExecuteNonQuery();

        Connection.Invoking(dbConnection =>
        {
            using var appender = dbConnection.CreateAppender("managedAppenderWrongTypeTest");
            var row = appender.CreateRow();
            row
                .AppendValue(false)
                .AppendValue(1)
                .AppendValue(Guid.NewGuid())
                .EndRow();
        }).Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ClosedAppenderRejectsFurtherOperations()
    {
        var table = "CREATE TABLE managedAppenderClosedAdapterTest(a BOOLEAN, c Date, b TINYINT);";
        Command.CommandText = table;
        Command.ExecuteNonQuery();

        using var appender = Connection.CreateAppender("managedAppenderClosedAdapterTest");
        appender.Close();

        appender.Invoking(value => value.CreateRow())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("Appender is already closed");
        appender.Invoking(value => value.AppendRow(row => row.AppendValue(false).AppendNullValue().AppendNullValue()))
            .Should().Throw<InvalidOperationException>()
            .WithMessage("Appender is already closed");
        appender.Invoking(value => value.Clear())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("Appender is already closed");
        appender.Invoking(value => value.Close())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("Appender is already closed");
        appender.Invoking(value => value.Dispose()).Should().NotThrow();
    }

    [Fact]
    public void EnumNotValidValueThrowException()
    {
        Command.CommandText = GetCreateEnumTypeSql("enum_not_valid_value_test_enum", "test", 100);
        Command.ExecuteNonQuery();

        var table = "CREATE TABLE managedAppenderEnumNotValidValueTest(a enum_not_valid_value_test_enum);";
        Command.CommandText = table;
        Command.ExecuteNonQuery();

        Connection.Invoking(dbConnection =>
        {
            using var appender = dbConnection.CreateAppender("managedAppenderEnumNotValidValueTest");
            appender
                .CreateRow()
                .AppendValue("test12345")
                .EndRow();
        }).Should().Throw<InvalidOperationException>();

        Connection.Invoking(dbConnection =>
        {
            using var appender = dbConnection.CreateAppender("managedAppenderEnumNotValidValueTest");
            appender
                .CreateRow()
                .AppendValue(EnumNotValidValueTestEnum.NotValid)
                .EndRow();
        }).Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void TableWithSchema()
    {
        var schema = "CREATE SCHEMA managedAppenderTestSchema";
        Command.CommandText = schema;
        Command.ExecuteNonQuery();

        using (var duckDbCommand = Connection.CreateCommand())
        {
            var table = "CREATE TABLE managedAppenderTestSchema.managedAppenderTest(a BOOLEAN, b TINYINT, c SMALLINT, d INTEGER, e BIGINT, f UTINYINT, g USMALLINT, h UINTEGER, i UBIGINT, j REAL, k DOUBLE, l VARCHAR, m Date);";
            duckDbCommand.CommandText = table;
            duckDbCommand.ExecuteNonQuery();
        }

        var rows = 10;
        using (var appender = Connection.CreateAppender("managedAppenderTestSchema", "managedAppenderTest"))
        {
            for (var i = 0; i < rows; i++)
            {
                var row = appender.CreateRow();
                row
                    .AppendValue(i % 2 == 0)
                    .AppendValue((sbyte?)i)
                    .AppendValue((short?)i)
                    .AppendValue((int?)i)
                    .AppendValue((long?)i)
                    .AppendValue((byte?)i)
                    .AppendValue((ushort?)i)
                    .AppendValue((uint?)i)
                    .AppendValue((ulong?)i)
                    .AppendValue((float)i)
                    .AppendValue((double)i)
                    .AppendValue($"{i}")
                    .AppendNullValue()
                    .EndRow();
            }
        }

        Command.CommandText = "SELECT * FROM managedAppenderTestSchema.managedAppenderTest";
        using (var reader = Command.ExecuteReader())
        {
            var readRowIndex = 0;
            while (reader.Read())
            {
                var booleanCell = (bool)reader[0];

                booleanCell.Should().Be(readRowIndex % 2 == 0);

                for (int columnIndex = 1; columnIndex < 12; columnIndex++)
                {
                    var cell = (IConvertible)reader[columnIndex];
                    cell.ToInt32(CultureInfo.InvariantCulture).Should().Be(readRowIndex);
                }

                readRowIndex++;
            }
            readRowIndex.Should().Be(rows);
        }
    }

    [Fact]
    public void ManagedAppenderOnTableAndColumns()
    {
        var table = "CREATE TABLE managedAppenderTest2(a INTEGER, b VARCHAR, c DateTime);";
        Command.CommandText = table;
        Command.ExecuteNonQuery();

        var rows = 5000;
        var date = DateTime.Today;

        var categories = Enumerable.Range(0, rows)
            .Select(i => new { id = i, name = Faker.Lorem.Word().OrNull(Faker), date = date.AddDays(i) })
            .ToList();

        using (var appender = Connection.CreateAppender("managedAppenderTest2"))
        {
            foreach (var item in categories)
            {
                var row = appender.CreateRow();
                row
                    .AppendValue(item.id)
                    .AppendValue(item.name)
                    .AppendValue(item.date)
                    .EndRow();
            }
        }

        var list = Connection.Query<(int id, string name, DateTime date)>("SELECT a, b, c FROM managedAppenderTest2").Select(tuple => new { tuple.id, tuple.name, tuple.date }).ToList();

        list.Should().HaveCount(rows);
        list.Should().BeEquivalentTo(categories);
    }

    [Theory]
    [InlineData("")]
    [InlineData("MY # SÇHËMÁ")]
    public void ManagedAppenderOnTableAndColumnsWithSpecialCharacters(string schemaName)
    {
        if (!string.IsNullOrWhiteSpace(schemaName))
        {
            var schema = $"CREATE SCHEMA {GetQualifiedObjectName(schemaName)}";
            Command.CommandText = schema;
            Command.ExecuteNonQuery();
        }

        var specialTableName = "SPÉçÏÃL - TÁBLÈ_";
        var specialColumnName = "SPÉçÏÃL @ CÓlümn";
        var specialStringValues = new[] { "Válüe 1", "Öthér V@L", "Lãst" };

        Command.CommandText = $"CREATE TABLE {GetQualifiedObjectName(schemaName, specialTableName)} ({GetQualifiedObjectName(specialColumnName)} TEXT)";
        Command.ExecuteNonQuery();

        using (var appender = Connection.CreateAppender(schemaName, specialTableName))
        {
            foreach (var spValue in specialStringValues)
            {
                var row = appender.CreateRow();
                row.AppendValue(spValue);
                row.EndRow();
            }
        }

        Command.CommandText = $"SELECT {GetQualifiedObjectName(specialTableName, specialColumnName)} FROM {GetQualifiedObjectName(schemaName, specialTableName)}";
        using (var reader = Command.ExecuteReader())
        {
            var colOrdinal = reader.GetOrdinal(specialColumnName);
            colOrdinal.Should().Be(0);

            var valueIdx = 0;
            while (reader.Read())
            {
                var expected = specialStringValues[valueIdx];
                reader.GetString(colOrdinal).Should().BeEquivalentTo(expected);
                valueIdx++;
            }
        }
    }

    [Fact]
    public void ManagedAppenderAppendToAttachedDatabase()
    {
        Command.CommandText = "ATTACH 'append_to_other.db'";
        Command.ExecuteNonQuery();

        Command.CommandText = "CREATE OR REPLACE TABLE append_to_other.tbl(i INTEGER)";
        Command.ExecuteNonQuery();

        var appender = Connection.CreateAppender("append_to_other", "main", "tbl");
        {
            for (int i = 0; i < 200; i++)
            {
                appender.CreateRow().AppendValue((int?)2).EndRow();
            }
            appender.Close();
        }

        var sum = Connection.QuerySingle<int>("SELECT sum(i)::BIGINT FROM append_to_other.main.tbl");
        sum.Should().Be(400);
    }

    [Fact]
    public void AppendDefault()
    {
        Command.CommandText = "CREATE OR REPLACE TABLE tbl (i INT DEFAULT 4, j INT, k INT DEFAULT 30)";
        Command.ExecuteNonQuery();

        using (var appender = Connection.CreateAppender("tbl"))
        {
            appender.CreateRow().AppendValue((int?)2).AppendValue(2).AppendDefault().EndRow();
            appender.CreateRow().AppendDefault().AppendValue(2).AppendDefault().EndRow();
        }

        Command.CommandText = "Select * from tbl";
        var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetInt32(0).Should().Be(2);
        reader.GetInt32(2).Should().Be(30);
        reader.Read();

        reader.GetInt32(0).Should().Be(4);
        reader.GetInt32(2).Should().Be(30);
    }

    [Fact]
    public void CreateRowReturnsIndependentRows()
    {
        Command.CommandText = "CREATE TABLE managedAppenderRowLifetime(a INTEGER, b INTEGER)";
        Command.ExecuteNonQuery();

        using (var appender = Connection.CreateAppender("managedAppenderRowLifetime"))
        {
            var completed = appender.CreateRow();
            completed.AppendValue((int?)10).AppendValue((int?)11).EndRow();

            var current = appender.CreateRow();

            completed.Should().NotBeSameAs(current);
            completed.Invoking(row => row.AppendValue((int?)99)).Should().Throw<IndexOutOfRangeException>();
            current.AppendValue((int?)20).AppendValue((int?)21).EndRow();
        }

        Command.CommandText = "SELECT a, b FROM managedAppenderRowLifetime ORDER BY a";
        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(10);
        reader.GetInt32(1).Should().Be(11);
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(20);
        reader.GetInt32(1).Should().Be(21);
        reader.Read().Should().BeFalse();
    }

    [Fact]
    public void AppendRowStateOverloadWritesRows()
    {
        Command.CommandText = "CREATE TABLE managedAppenderScopedRow(a INTEGER, b VARCHAR)";
        Command.ExecuteNonQuery();

        using (var appender = Connection.CreateAppender("managedAppenderScopedRow"))
        {
            for (var i = 0; i < 3; i++)
            {
                appender.AppendRow((Id: i, Name: $"row-{i}"), static (row, value) =>
                {
                    row.AppendValue(value.Id).AppendValue(value.Name);
                });
            }
        }

        Command.CommandText = "SELECT a, b FROM managedAppenderScopedRow ORDER BY a";
        using var reader = Command.ExecuteReader();
        for (var i = 0; i < 3; i++)
        {
            reader.Read().Should().BeTrue();
            reader.GetInt32(0).Should().Be(i);
            reader.GetString(1).Should().Be($"row-{i}");
        }

        reader.Read().Should().BeFalse();
    }

    [Fact]
    public void AppendRowActionOverloadWritesCompleteRow()
    {
        Command.CommandText = "CREATE TABLE managedAppenderActionRow(a INTEGER, b VARCHAR)";
        Command.ExecuteNonQuery();

        using (var appender = Connection.CreateAppender("managedAppenderActionRow"))
        {
            appender.AppendRow(row => row.AppendValue((int?)42).AppendValue("answer"));
        }

        Command.CommandText = "SELECT a, b FROM managedAppenderActionRow";
        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(42);
        reader.GetString(1).Should().Be("answer");
        reader.Read().Should().BeFalse();
    }

    [Fact]
    public void AppendRowActionOverloadRejectsNullCallback()
    {
        Command.CommandText = "CREATE TABLE managedAppenderNullAction(a INTEGER)";
        Command.ExecuteNonQuery();

        using var appender = Connection.CreateAppender("managedAppenderNullAction");
        appender.Invoking(value => value.AppendRow((Action<IDuckDBAppenderRow>)null!))
            .Should().Throw<ArgumentNullException>()
            .WithParameterName("writeRow");
    }

    [Fact]
    public void AppendRowStateOverloadRejectsNullCallback()
    {
        Command.CommandText = "CREATE TABLE managedAppenderNullStateAction(a INTEGER)";
        Command.ExecuteNonQuery();

        using var appender = Connection.CreateAppender("managedAppenderNullStateAction");
        appender.Invoking(value => value.AppendRow(1, (Action<IDuckDBAppenderRow, int>)null!))
            .Should().Throw<ArgumentNullException>()
            .WithParameterName("writeRow");
    }

    [Fact]
    public void IncompleteAppendRowDiscardsFailedRowAndFaultsAppender()
    {
        Command.CommandText = "CREATE TABLE managedAppenderIncompleteScopedRow(a INTEGER, b INTEGER)";
        Command.ExecuteNonQuery();

        using (var appender = Connection.CreateAppender("managedAppenderIncompleteScopedRow"))
        {
            appender.Invoking(value => value.AppendRow(1, static (row, state) => row.AppendValue(state)))
                .Should().Throw<InvalidOperationException>()
                .WithMessage("*specified only 1 values");

            appender.Invoking(value => value.AppendRow((2, 3), static (row, state) =>
                    row.AppendValue(state.Item1).AppendValue(state.Item2)))
                .Should().Throw<InvalidOperationException>()
                .WithMessage("*cannot be reused*");
        }

        Command.CommandText = "SELECT a, b FROM managedAppenderIncompleteScopedRow";
        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeFalse();
    }

    [Fact]
    public void AppendRowFailureDiscardsFailedRowAndCommitsCompletedRowsOnClose()
    {
        Command.CommandText = "CREATE TABLE managedAppenderThrownScopedRow(a INTEGER, b INTEGER[])";
        Command.ExecuteNonQuery();

        IDuckDBAppenderRow failedRow = null!;

        using (var appender = Connection.CreateAppender("managedAppenderThrownScopedRow"))
        {
            appender.AppendRow(row => row.AppendValue((int?)1).AppendValue(new[] { 1, 2 }));

            appender.Invoking(value => value.AppendRow(row =>
                {
                    failedRow = row;
                    row.AppendValue((int?)2).AppendValue(new[] { 3, 4 });
                    throw new InvalidOperationException("callback failed");
                }))
                .Should().Throw<InvalidOperationException>()
                .WithMessage("callback failed");

            failedRow.Should().NotBeNull();
            failedRow.Invoking(row => row.AppendValue((int?)99)).Should().Throw<IndexOutOfRangeException>();

            appender.Invoking(value => value.AppendRow(row => row.AppendValue((int?)3).AppendValue(new[] { 5, 6 })))
                .Should().Throw<InvalidOperationException>()
                .WithMessage("*cannot be reused*");

            appender.Invoking(value => value.CreateRow())
                .Should().Throw<InvalidOperationException>()
                .WithMessage("*cannot be reused*");

            appender.Invoking(value => value.Clear())
                .Should().Throw<InvalidOperationException>()
                .WithMessage("*cannot be reused*");

            // Close is still allowed on a faulted appender: it commits the rows completed before
            // the failure (the failed row was discarded).
            appender.Invoking(value => value.Close()).Should().NotThrow();
        }

        Command.CommandText = "SELECT a, b FROM managedAppenderThrownScopedRow ORDER BY a";
        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);
        reader.GetFieldValue<List<int>>(1).Should().Equal(1, 2);
        reader.Read().Should().BeFalse();
    }

    [Fact]
    public void AppendRowFailureSurfacesCompletedRowCommitErrorAtClose()
    {
        Command.CommandText = "CREATE TABLE managedAppenderFailedFinalization(a INTEGER UNIQUE)";
        Command.ExecuteNonQuery();
        Command.CommandText = "INSERT INTO managedAppenderFailedFinalization VALUES (1)";
        Command.ExecuteNonQuery();

        using var appender = Connection.CreateAppender("managedAppenderFailedFinalization");

        // This completed row duplicates the existing key; it stays buffered (uncommitted).
        appender.AppendRow(row => row.AppendValue((int?)1));

        // The failing row is dropped and the appender faults. The callback exception propagates
        // on its own - the failure itself does not commit anything.
        appender.Invoking(value => value.AppendRow(row =>
            {
                row.AppendValue((int?)2);
                throw new InvalidOperationException("callback failed");
            }))
            .Should().Throw<InvalidOperationException>()
            .WithMessage("callback failed");

        // Committing the completed row happens at Close, and that is where the constraint
        // violation surfaces.
        appender.Invoking(value => value.Close())
            .Should().Throw<DuckDBException>()
            .Which.ErrorType.Should().Be(DuckDBErrorType.Constraint);

        // Close already tore the appender down, so the using-block dispose is a no-op.
        appender.Invoking(value => value.Dispose()).Should().NotThrow();

        Command.CommandText = "SELECT count(*) FROM managedAppenderFailedFinalization";
        Command.ExecuteScalar().Should().Be(1);
    }

    [Fact]
    public void AppendRowFailureFlushesCompletedRowsAcrossDataChunks()
    {
        Command.CommandText = "CREATE TABLE managedAppenderFailedAcrossChunks(a INTEGER)";
        Command.ExecuteNonQuery();

        var completedRowCount = checked((int)DuckDBGlobalData.VectorSize + 3);

        using (var appender = Connection.CreateAppender("managedAppenderFailedAcrossChunks"))
        {
            for (var i = 0; i < completedRowCount; i++)
            {
                appender.AppendRow(i, static (row, value) => row.AppendValue(value));
            }

            appender.Invoking(value => value.AppendRow(completedRowCount, static (row, failedValue) =>
                {
                    row.AppendValue(failedValue);
                    throw new InvalidOperationException("callback failed");
                }))
                .Should().Throw<InvalidOperationException>()
                .WithMessage("callback failed");
        }

        Command.CommandText = """
                              SELECT count(*)::BIGINT, min(a), max(a), sum(a)::BIGINT
                              FROM managedAppenderFailedAcrossChunks
                              """;
        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetInt64(0).Should().Be(completedRowCount);
        reader.GetInt32(1).Should().Be(0);
        reader.GetInt32(2).Should().Be(completedRowCount - 1);
        reader.GetInt64(3).Should().Be((long)(completedRowCount - 1) * completedRowCount / 2);
    }

    [Fact]
    public void AppendRowFailureLeavesCompletedRowsInCallerTransaction()
    {
        Command.CommandText = "CREATE TABLE managedAppenderFailedTransaction(a INTEGER)";
        Command.ExecuteNonQuery();

        using (var transaction = Connection.BeginTransaction())
        {
            using (var appender = Connection.CreateAppender("managedAppenderFailedTransaction"))
            {
                appender.AppendRow(row => row.AppendValue((int?)1));

                appender.Invoking(value => value.AppendRow(row =>
                    {
                        row.AppendValue((int?)2);
                        throw new InvalidOperationException("callback failed");
                    }))
                    .Should().Throw<InvalidOperationException>()
                    .WithMessage("callback failed");
            }

            Command.CommandText = "SELECT count(*) FROM managedAppenderFailedTransaction";
            Command.ExecuteScalar().Should().Be(1);

            transaction.Rollback();
        }

        Command.CommandText = "SELECT count(*) FROM managedAppenderFailedTransaction";
        Command.ExecuteScalar().Should().Be(0);
    }

    [Fact]
    public void AppendRowWritesListValue()
    {
        Command.CommandText = "CREATE TABLE managedAppenderScopedListRow(a INTEGER, b INTEGER[])";
        Command.ExecuteNonQuery();

        using (var appender = Connection.CreateAppender("managedAppenderScopedListRow"))
        {
            appender.AppendRow(row => row.AppendValue((int?)1).AppendValue(new[] { 1, 2 }));
        }

        Command.CommandText = "SELECT a, b FROM managedAppenderScopedListRow";
        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);
        reader.GetFieldValue<List<int>>(1).Should().Equal(1, 2);
        reader.Read().Should().BeFalse();
    }

    [Fact]
    public void AppendRowRejectsReentrantAppenderUseAndFaultsAppender()
    {
        Command.CommandText = "CREATE TABLE managedAppenderReentrantScopedRow(a INTEGER)";
        Command.ExecuteNonQuery();

        using (var appender = Connection.CreateAppender("managedAppenderReentrantScopedRow"))
        {
            appender.Invoking(value => value.AppendRow(row =>
                {
                    row.AppendValue((int?)1);
                    value.AppendRow(nested => nested.AppendValue((int?)2));
                }))
                .Should().Throw<InvalidOperationException>()
                .WithMessage("*inside an AppendRow callback");

            appender.Invoking(value => value.AppendRow(row => row.AppendValue((int?)3)))
                .Should().Throw<InvalidOperationException>()
                .WithMessage("*cannot be reused*");
        }

        Command.CommandText = "SELECT a FROM managedAppenderReentrantScopedRow";
        using var reader = Command.ExecuteReader();
        reader.Read().Should().BeFalse();
    }

    [Fact]
    public void ClearAppender()
    {
        Command.CommandText = "CREATE OR REPLACE TABLE tbl_empty (i INT DEFAULT 4, j INT, k INT DEFAULT 30)";
        Command.ExecuteNonQuery();

        using (var appender = Connection.CreateAppender("tbl_empty"))
        {
            for (int i = 0; i < 10_000; i++)
            {
                appender.CreateRow().AppendValue((int?)2).AppendValue(2).AppendDefault().EndRow();
            }

            appender.Clear();
        }

        Command.CommandText = "Select count(*) from tbl_empty";
        var count = Command.ExecuteScalar();
        count.Should().Be(0);
    }


    [Fact]
    public void ClearAppenderAddMoreData()
    {
        Command.CommandText = "CREATE OR REPLACE TABLE tbl_empty (i INT DEFAULT 4, j INT, k INT DEFAULT 30)";
        Command.ExecuteNonQuery();

        using (var appender = Connection.CreateAppender("tbl_empty"))
        {
            for (int i = 0; i < 10_000; i++)
            {
                appender.CreateRow().AppendValue((int?)2).AppendValue(2).AppendDefault().EndRow();
            }

            appender.Clear();

            for (int i = 0; i < 5_000; i++)
            {
                appender.CreateRow().AppendValue((int?)3).AppendValue(3).AppendDefault().EndRow();
            }
        }

        Command.CommandText = "Select count(*) from tbl_empty";
        var count = Command.ExecuteScalar();
        count.Should().Be(5000);
    }

    [Fact]
    public void AppenderWithEnum()
    {
        Command.CommandText = "CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');";
        Command.ExecuteNonQuery();

        Command.CommandText = "CREATE TABLE managedAppenderEnumChunkBoundary (id INTEGER, feeling mood);";
        Command.ExecuteNonQuery();

        var moods = new[] { "happy", "sad", "neutral" };

        // Append more than 2048 rows (VectorSize) to trigger writer recreation across the chunk boundary
        const int rows = 2049;
        using (var appender = Connection.CreateAppender("managedAppenderEnumChunkBoundary"))
        {
            for (int i = 0; i < rows; i++)
            {
                appender.CreateRow()
                    .AppendValue(i)
                    .AppendValue(moods[i % moods.Length])
                    .EndRow();
            }
        }

        Command.CommandText = "SELECT id, feeling::VARCHAR FROM managedAppenderEnumChunkBoundary ORDER BY id";
        using var reader = Command.ExecuteReader();

        for (int i = 0; i < rows; i++)
        {
            reader.Read().Should().BeTrue();
            reader.GetInt32(0).Should().Be(i);
            reader.GetString(1).Should().Be(moods[i % moods.Length]);
        }

        reader.Read().Should().BeFalse();
    }

    private static string GetCreateEnumTypeSql(string enumName, string enumValueNamePrefix, int count)
    {
        var stringBuilder = new StringBuilder();
        stringBuilder.AppendFormat(CultureInfo.InvariantCulture, "CREATE TYPE {0} AS ENUM(", enumName);

        for (int i = 1; i <= count; i++)
        {
            if (i > 1)
            {
                stringBuilder.Append(',');
            }

            stringBuilder.Append('\'');
            stringBuilder.Append(enumValueNamePrefix);
            stringBuilder.Append(i);
            stringBuilder.Append('\'');
        }

        stringBuilder.Append(");");
        return stringBuilder.ToString();
    }

    private static string GetQualifiedObjectName(params string[] parts) =>
        string.Join('.', parts.
            Where(p => !string.IsNullOrWhiteSpace(p)).
            Select(p => '"' + p + '"')
        );

    private enum TestEnum1
    {
        Test1 = 0,
        Test2 = 1,
        Test3 = 2,
    }

    private enum TestEnum2 : short
    {
        Test327 = 326,
        Test1000 = 999,
    }

    private enum TestEnum3 : ulong
    {
        Test6699 = 6698,
        Test100000 = 99999,
    }

    private enum EnumNotValidValueTestEnum
    {
        NotValid = 12345,
    }
}
