using System;
using System.Globalization;
using DuckDB.NET.Data;
using Xunit;
using FluentAssertions;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Numerics;
using Dapper;
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
    public void GuidValues()
    {
        Command.CommandText = "CREATE TABLE managedAppenderGuids(a UUID);";
        Command.ExecuteNonQuery();

        var guids = Enumerable.Range(0, 20).Select(i => (Guid?)Guid.NewGuid()).ToList();
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

        var timeSpans = Enumerable.Range(0, 20).Select(i => TimeSpan.FromSeconds(Random.Shared.Next(1_000_000, 1_000_000 * 10))).ToList();

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
        Command.CommandText = "CREATE TABLE managedAppenderTemporal(a Date, b TimeStamp, c TIMESTAMP_NS, d TIMESTAMP_MS, e TIMESTAMP_S, f TIMESTAMPTZ, g TIMETZ, h Time);";
        Command.ExecuteNonQuery();

        var dates = Enumerable.Range(0, 20).Select(i => new DateTime(1900, 1, 1).AddDays(Random.Shared.Next(1, 50000)).AddSeconds(Random.Shared.Next(3600 * 2, 3600 * 24))).ToList();

        using (var appender = Connection.CreateAppender("managedAppenderTemporal"))
        {
            foreach (var value in dates)
            {
                appender.CreateRow()
                                .AppendValue((DateOnly?)DateOnly.FromDateTime(value))
                                .AppendValue(value).AppendValue(value)
                                .AppendValue(value).AppendValue(value).AppendValue(value)
                                .AppendValue(value.ToDateTimeOffset(TimeSpan.FromHours(1)))
                                .AppendValue((TimeOnly?)TimeOnly.FromDateTime(value))
                        .EndRow();
            }
        }

        var result = Connection.Query<(DateOnly, DateTime, DateTime, DateTime, DateTime, DateTime, DateTimeOffset, TimeOnly)>("SELECT a, b, c, d, e, f, g, h FROM managedAppenderTemporal").ToList();

        result.Select(tuple => tuple.Item1).Should().BeEquivalentTo(dates.Select(DateOnly.FromDateTime));
        result.Select(tuple => tuple.Item2).Should().BeEquivalentTo(dates);
        result.Select(tuple => tuple.Item3).Should().BeEquivalentTo(dates);
        result.Select(tuple => tuple.Item4).Should().BeEquivalentTo(dates);
        result.Select(tuple => tuple.Item5).Should().BeEquivalentTo(dates);
        result.Select(tuple => tuple.Item6).Should().BeEquivalentTo(dates);
        result.Select(tuple => tuple.Item7).Should().BeEquivalentTo(dates.Select(time => time.ToDateTimeOffset(TimeSpan.FromHours(1))), 
                                                              options => options.ComparingByMembers<DateTimeOffset>().Including(offset => offset.Offset).Including(offset => offset.TimeOfDay));
        result.Select(tuple => tuple.Item8).Should().BeEquivalentTo(dates.Select(TimeOnly.FromDateTime));
    }

    [Fact]
    public void ListValues()
    {
        Command.CommandText = "CREATE TABLE managedAppenderLists(a INTEGER, b INTEGER[]" +
                              ", c INTEGER[][]" +
                              ");";
        Command.ExecuteNonQuery();

        var rows = 2;
        using (var appender = Connection.CreateAppender("managedAppenderLists"))
        {
            for (int i = 0; i < rows; i++)
            {
                appender.CreateRow()
                    .AppendValue(i)
                    .AppendValue(Enumerable.Range(0, 2).ToList())
                    .AppendValue(new List<List<int>> { Enumerable.Range(0, 5).ToList(), Enumerable.Range(i + 2, 4).ToList() })
                    .EndRow();
            }
        }

        Command.CommandText = "SELECT * FROM managedAppenderLists order by 1";
        var reader = Command.ExecuteReader();

        int index = 0;
        while (reader.Read())
        {
            var ints = reader.GetFieldValue<List<int>>(1);
            ints.Should().BeEquivalentTo(Enumerable.Range(0, 2).ToList());
            var fieldValue = reader.GetFieldValue<List<List<int>>>(2);
        }
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
    public void ClosedAdapterThrowException()
    {
        var table = "CREATE TABLE managedAppenderClosedAdapterTest(a BOOLEAN, c Date, b TINYINT);";
        Command.CommandText = table;
        Command.ExecuteNonQuery();

        Connection.Invoking(dbConnection =>
        {
            using var appender = dbConnection.CreateAppender("managedAppenderClosedAdapterTest");
            appender.Close();
            var row = appender.CreateRow();
            row
                .AppendValue(false)
                .AppendValue((byte)1)
                .AppendValue((short?)1)
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
        var specialStringValues = new string[] { "Válüe 1", "Öthér V@L", "Lãst" };

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

    private static string GetQualifiedObjectName(params string[] parts) =>
        string.Join('.', parts.
            Where(p => !string.IsNullOrWhiteSpace(p)).
            Select(p => '"' + p + '"')
        );
}