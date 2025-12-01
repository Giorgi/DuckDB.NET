using DuckDB.NET.Data;
using DuckDB.NET.Native;
using FluentAssertions;
using System;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBDataReaderInfinityTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void duckdb_date_is_finite()
    {
        var positiveInfinity = new DuckDBDate { Days = Int32.MaxValue };
        var negativeInfinity = new DuckDBDate { Days = -Int32.MaxValue }; // NOTE: -MaxValue, not MinValue
        var finitePositive = new DuckDBDate { Days = Int32.MaxValue - 1 };
        var finiteNegative = new DuckDBDate { Days = -Int32.MaxValue + 1 };
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteDate(finitePositive).Should().BeTrue();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteDate(positiveInfinity).Should().BeFalse();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteDate(finiteNegative).Should().BeTrue();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteDate(negativeInfinity).Should().BeFalse();
    }

    [Fact]
    public void duckdb_timestamp_is_finite()
    {
        var positiveInfinity = new DuckDBTimestampStruct { Micros = Int64.MaxValue };
        var negativeInfinity = new DuckDBTimestampStruct { Micros = -Int64.MaxValue };
        var finitePositive = new DuckDBTimestampStruct { Micros = Int64.MaxValue - 1 };
        var finiteNegative = new DuckDBTimestampStruct { Micros = -Int64.MaxValue + 1 };
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestamp(finitePositive).Should().BeTrue();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestamp(positiveInfinity).Should().BeFalse();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestamp(finiteNegative).Should().BeTrue();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestamp(negativeInfinity).Should().BeFalse();
    }

    [Fact]
    public void duckdb_timestamp_s_is_finite()
    {
        var positiveInfinity = new DuckDBTimestampStruct { Micros = Int64.MaxValue };
        var negativeInfinity = new DuckDBTimestampStruct { Micros = -Int64.MaxValue };
        var finitePositive = new DuckDBTimestampStruct { Micros = Int64.MaxValue - 1 };
        var finiteNegative = new DuckDBTimestampStruct { Micros = -Int64.MaxValue + 1 };
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampS(finitePositive).Should().BeTrue();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampS(positiveInfinity).Should().BeFalse();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampS(finiteNegative).Should().BeTrue();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampS(negativeInfinity).Should().BeFalse();
    }

    [Fact]
    public void duckdb_timestamp_ms_is_finite()
    {
        var positiveInfinity = new DuckDBTimestampStruct { Micros = Int64.MaxValue };
        var negativeInfinity = new DuckDBTimestampStruct { Micros = -Int64.MaxValue };
        var finitePositive = new DuckDBTimestampStruct { Micros = Int64.MaxValue - 1 };
        var finiteNegative = new DuckDBTimestampStruct { Micros = -Int64.MaxValue + 1 };
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampMs(finitePositive).Should().BeTrue();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampMs(positiveInfinity).Should().BeFalse();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampMs(finiteNegative).Should().BeTrue();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampMs(negativeInfinity).Should().BeFalse();
    }

    [Fact]
    public void duckdb_timestamp_ns_is_finite()
    {
        var positiveInfinity = new DuckDBTimestampStruct { Micros = Int64.MaxValue };
        var negativeInfinity = new DuckDBTimestampStruct { Micros = -Int64.MaxValue };
        var finitePositive = new DuckDBTimestampStruct { Micros = Int64.MaxValue - 1 };
        var finiteNegative = new DuckDBTimestampStruct { Micros = -Int64.MaxValue + 1 };
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampNs(finitePositive).Should().BeTrue();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampNs(positiveInfinity).Should().BeFalse();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampNs(finiteNegative).Should().BeTrue();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampNs(negativeInfinity).Should().BeFalse();
    }

    [Fact]
    public void ReadInfinityDate()
    {
        Command.CommandText = "SELECT 'infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetDateTime(0).Should().Be(DateTime.MaxValue);
        reader.GetFieldValue<DateTime>(0).Should().Be(DateTime.MaxValue);
    }

    [Fact]
    public void ReadNegativeInfinityDate()
    {
        Command.CommandText = "SELECT '-infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetDateTime(0).Should().Be(DateTime.MinValue);
        reader.GetFieldValue<DateTime>(0).Should().Be(DateTime.MinValue);
    }

#if NET6_0_OR_GREATER
    [Fact]
    public void ReadInfinityDateAsDateOnly()
    {
        Command.CommandText = "SELECT 'infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DateOnly>(0).Should().Be(DateOnly.MaxValue);
    }

    [Fact]
    public void ReadNegativeInfinityDateAsDateOnly()
    {
        Command.CommandText = "SELECT '-infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DateOnly>(0).Should().Be(DateOnly.MinValue);
    }
#endif

    [Fact]
    public void ReadInfinityTimestamp()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP, 'infinity'::TIMESTAMP_NS, 'infinity'::TIMESTAMP_MS, 'infinity'::TIMESTAMP_S";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetDateTime(0).Should().Be(DateTime.MaxValue);
        reader.GetFieldValue<DateTime>(0).Should().Be(DateTime.MaxValue);
        reader.GetDateTime(1).Should().Be(DateTime.MaxValue);
        reader.GetFieldValue<DateTime>(1).Should().Be(DateTime.MaxValue);
        reader.GetDateTime(2).Should().Be(DateTime.MaxValue);
        reader.GetFieldValue<DateTime>(2).Should().Be(DateTime.MaxValue);
        reader.GetDateTime(3).Should().Be(DateTime.MaxValue);
        reader.GetFieldValue<DateTime>(3).Should().Be(DateTime.MaxValue);
    }

    [Fact]
    public void ReadNegativeInfinityTimestamp()
    {
        Command.CommandText = "SELECT '-infinity'::TIMESTAMP, '-infinity'::TIMESTAMP_NS, '-infinity'::TIMESTAMP_MS, '-infinity'::TIMESTAMP_S";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetDateTime(0).Should().Be(DateTime.MinValue);
        reader.GetFieldValue<DateTime>(0).Should().Be(DateTime.MinValue);
        reader.GetDateTime(1).Should().Be(DateTime.MinValue);
        reader.GetFieldValue<DateTime>(1).Should().Be(DateTime.MinValue);
        reader.GetDateTime(2).Should().Be(DateTime.MinValue);
        reader.GetFieldValue<DateTime>(2).Should().Be(DateTime.MinValue);
        reader.GetDateTime(3).Should().Be(DateTime.MinValue);
        reader.GetFieldValue<DateTime>(3).Should().Be(DateTime.MinValue);
    }

    [Fact]
    public void ReadInfinityTimestampAsDateTimeOffset()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DateTimeOffset>(0).Should().Be(new DateTimeOffset(DateTime.MaxValue, TimeSpan.Zero));
    }

    [Fact]
    public void ReadNegativeInfinityTimestampAsDateTimeOffset()
    {
        Command.CommandText = "SELECT '-infinity'::TIMESTAMP";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DateTimeOffset>(0).Should().Be(new DateTimeOffset(DateTime.MinValue, TimeSpan.Zero));
    }

    [Fact]
    public void ReadInfinityTimestampTz()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMPTZ";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetDateTime(0).Should().Be(DateTime.MaxValue);
        reader.GetFieldValue<DateTime>(0).Should().Be(DateTime.MaxValue);
    }

    [Fact]
    public void ReadNegativeInfinityTimestampTz()
    {
        Command.CommandText = "SELECT '-infinity'::TIMESTAMPTZ";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetDateTime(0).Should().Be(DateTime.MinValue);
        reader.GetFieldValue<DateTime>(0).Should().Be(DateTime.MinValue);
    }

    [Fact]
    public void ReadInfinityTimestampTzAsDateTimeOffset()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMPTZ";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DateTimeOffset>(0).Should().Be(new DateTimeOffset(DateTime.MaxValue, TimeSpan.Zero));
    }

    [Fact]
    public void ReadNegativeInfinityTimestampTzAsDateTimeOffset()
    {
        Command.CommandText = "SELECT '-infinity'::TIMESTAMPTZ";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DateTimeOffset>(0).Should().Be(new DateTimeOffset(DateTime.MinValue, TimeSpan.Zero));
    }

    [Fact]
    public void ReadMixedInfinityDates()
    {
        Command.CommandText = "SELECT * FROM (VALUES ('infinity'::DATE), ('-infinity'::DATE), ('2024-01-15'::DATE)) AS t(d)";
        using var reader = Command.ExecuteReader();

        reader.Read();
        reader.GetDateTime(0).Should().Be(DateTime.MaxValue);

        reader.Read();
        reader.GetDateTime(0).Should().Be(DateTime.MinValue);

        reader.Read();
        reader.GetDateTime(0).Should().Be(new DateTime(2024, 1, 15));
    }

    [Fact]
    public void ReadMixedInfinityTimestamps()
    {
        Command.CommandText = "SELECT * FROM (VALUES ('infinity'::TIMESTAMP), ('-infinity'::TIMESTAMP), ('2024-01-15 12:30:45'::TIMESTAMP)) AS t(ts)";
        using var reader = Command.ExecuteReader();

        reader.Read();
        reader.GetDateTime(0).Should().Be(DateTime.MaxValue);

        reader.Read();
        reader.GetDateTime(0).Should().Be(DateTime.MinValue);

        reader.Read();
        reader.GetDateTime(0).Should().Be(new DateTime(2024, 1, 15, 12, 30, 45));
    }

    [Fact]
    public void ReadNullableInfinityDate()
    {
        Command.CommandText = "SELECT 'infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DateTime?>(0).Should().Be(DateTime.MaxValue);
    }

    [Fact]
    public void ReadNullableNegativeInfinityDate()
    {
        Command.CommandText = "SELECT '-infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DateTime?>(0).Should().Be(DateTime.MinValue);
    }

    [Fact]
    public void ReadNullableInfinityTimestamp()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DateTime?>(0).Should().Be(DateTime.MaxValue);
    }

    [Fact]
    public void ReadNullableNegativeInfinityTimestamp()
    {
        Command.CommandText = "SELECT '-infinity'::TIMESTAMP";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DateTime?>(0).Should().Be(DateTime.MinValue);
    }

#if NET6_0_OR_GREATER
    [Fact]
    public void ReadNullableInfinityDateAsDateOnly()
    {
        Command.CommandText = "SELECT 'infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DateOnly?>(0).Should().Be(DateOnly.MaxValue);
    }

    [Fact]
    public void ReadNullableNegativeInfinityDateAsDateOnly()
    {
        Command.CommandText = "SELECT '-infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DateOnly?>(0).Should().Be(DateOnly.MinValue);
    }
#endif

    [Fact]
    public void ReadNullableInfinityTimestampAsDateTimeOffset()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DateTimeOffset?>(0).Should().Be(new DateTimeOffset(DateTime.MaxValue, TimeSpan.Zero));
    }

    [Fact]
    public void ReadNullableNegativeInfinityTimestampAsDateTimeOffset()
    {
        Command.CommandText = "SELECT '-infinity'::TIMESTAMP";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DateTimeOffset?>(0).Should().Be(new DateTimeOffset(DateTime.MinValue, TimeSpan.Zero));
    }
}
