using DuckDB.NET.Data;
using DuckDB.NET.Native;
using FluentAssertions;
using System;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBDataReaderInfinityTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    #region Native IsFinite Tests

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

    #endregion

    #region Reading Infinity Dates as DuckDBDateOnly (Success Cases)

    [Fact]
    public void ReadInfinityDateAsDuckDBDateOnly()
    {
        Command.CommandText = "SELECT 'infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var dateOnly = reader.GetFieldValue<DuckDBDateOnly>(0);
        dateOnly.Should().Be(DuckDBDateOnly.PositiveInfinity);
        dateOnly.IsPositiveInfinity.Should().BeTrue();
        dateOnly.IsInfinity.Should().BeTrue();
    }

    [Fact]
    public void ReadNegativeInfinityDateAsDuckDBDateOnly()
    {
        Command.CommandText = "SELECT '-infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var dateOnly = reader.GetFieldValue<DuckDBDateOnly>(0);
        dateOnly.Should().Be(DuckDBDateOnly.NegativeInfinity);
        dateOnly.IsNegativeInfinity.Should().BeTrue();
        dateOnly.IsInfinity.Should().BeTrue();
    }

    #endregion

    #region Reading Infinity Dates as DateTime/DateOnly (Exception Cases)

    [Fact]
    public void ReadInfinityDateAsDateTime_ThrowsException()
    {
        Command.CommandText = "SELECT 'infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var act = () => reader.GetDateTime(0);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*infinite*DuckDBDateOnly*");
    }

    [Fact]
    public void ReadNegativeInfinityDateAsDateTime_ThrowsException()
    {
        Command.CommandText = "SELECT '-infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var act = () => reader.GetDateTime(0);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*infinite*DuckDBDateOnly*");
    }

    [Fact]
    public void ReadInfinityDateAsNullableDateTime_ThrowsException()
    {
        Command.CommandText = "SELECT 'infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var act = () => reader.GetFieldValue<DateTime?>(0);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*infinite*DuckDBDateOnly*");
    }

#if NET6_0_OR_GREATER
    [Fact]
    public void ReadInfinityDateAsDateOnly_ThrowsException()
    {
        Command.CommandText = "SELECT 'infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var act = () => reader.GetFieldValue<DateOnly>(0);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*infinite*DuckDBDateOnly*");
    }

    [Fact]
    public void ReadNegativeInfinityDateAsDateOnly_ThrowsException()
    {
        Command.CommandText = "SELECT '-infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var act = () => reader.GetFieldValue<DateOnly>(0);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*infinite*DuckDBDateOnly*");
    }

    [Fact]
    public void ReadInfinityDateAsNullableDateOnly_ThrowsException()
    {
        Command.CommandText = "SELECT 'infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var act = () => reader.GetFieldValue<DateOnly?>(0);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*infinite*DuckDBDateOnly*");
    }
#endif

    #endregion

    #region Reading Infinity Timestamps as DuckDBTimestamp (Success Cases)

    [Fact]
    public void ReadInfinityTimestampAsDuckDBTimestamp()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var timestamp = reader.GetFieldValue<DuckDBTimestamp>(0);
        timestamp.Should().Be(DuckDBTimestamp.PositiveInfinity);
        timestamp.IsPositiveInfinity.Should().BeTrue();
        timestamp.IsInfinity.Should().BeTrue();
    }

    [Fact]
    public void ReadNegativeInfinityTimestampAsDuckDBTimestamp()
    {
        Command.CommandText = "SELECT '-infinity'::TIMESTAMP";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var timestamp = reader.GetFieldValue<DuckDBTimestamp>(0);
        timestamp.Should().Be(DuckDBTimestamp.NegativeInfinity);
        timestamp.IsNegativeInfinity.Should().BeTrue();
        timestamp.IsInfinity.Should().BeTrue();
    }

    [Fact]
    public void ReadInfinityTimestampVariantsAsDuckDBTimestamp()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP_NS, 'infinity'::TIMESTAMP_MS, 'infinity'::TIMESTAMP_S";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DuckDBTimestamp>(0).IsPositiveInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBTimestamp>(1).IsPositiveInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBTimestamp>(2).IsPositiveInfinity.Should().BeTrue();
    }

    [Fact]
    public void ReadNegativeInfinityTimestampVariantsAsDuckDBTimestamp()
    {
        Command.CommandText = "SELECT '-infinity'::TIMESTAMP_NS, '-infinity'::TIMESTAMP_MS, '-infinity'::TIMESTAMP_S";
        using var reader = Command.ExecuteReader();
        reader.Read();

        reader.GetFieldValue<DuckDBTimestamp>(0).IsNegativeInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBTimestamp>(1).IsNegativeInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBTimestamp>(2).IsNegativeInfinity.Should().BeTrue();
    }

    [Fact]
    public void ReadInfinityTimestampTzAsDuckDBTimestamp()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMPTZ";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var timestamp = reader.GetFieldValue<DuckDBTimestamp>(0);
        timestamp.IsPositiveInfinity.Should().BeTrue();
    }

    [Fact]
    public void ReadNegativeInfinityTimestampTzAsDuckDBTimestamp()
    {
        Command.CommandText = "SELECT '-infinity'::TIMESTAMPTZ";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var timestamp = reader.GetFieldValue<DuckDBTimestamp>(0);
        timestamp.IsNegativeInfinity.Should().BeTrue();
    }

    #endregion

    #region Reading Infinity Timestamps as DateTime/DateTimeOffset (Exception Cases)

    [Fact]
    public void ReadInfinityTimestampAsDateTime_ThrowsException()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var act = () => reader.GetDateTime(0);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*infinite*DuckDBTimestamp*");
    }

    [Fact]
    public void ReadNegativeInfinityTimestampAsDateTime_ThrowsException()
    {
        Command.CommandText = "SELECT '-infinity'::TIMESTAMP";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var act = () => reader.GetDateTime(0);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*infinite*DuckDBTimestamp*");
    }

    [Fact]
    public void ReadInfinityTimestampAsNullableDateTime_ThrowsException()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var act = () => reader.GetFieldValue<DateTime?>(0);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*infinite*DuckDBTimestamp*");
    }

    [Fact]
    public void ReadInfinityTimestampAsDateTimeOffset_ThrowsException()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var act = () => reader.GetFieldValue<DateTimeOffset>(0);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*infinite*DuckDBTimestamp*");
    }

    [Fact]
    public void ReadNegativeInfinityTimestampAsDateTimeOffset_ThrowsException()
    {
        Command.CommandText = "SELECT '-infinity'::TIMESTAMP";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var act = () => reader.GetFieldValue<DateTimeOffset>(0);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*infinite*DuckDBTimestamp*");
    }

    [Fact]
    public void ReadInfinityTimestampAsNullableDateTimeOffset_ThrowsException()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var act = () => reader.GetFieldValue<DateTimeOffset?>(0);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*infinite*DuckDBTimestamp*");
    }

    [Fact]
    public void ReadInfinityTimestampTzAsDateTime_ThrowsException()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMPTZ";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var act = () => reader.GetDateTime(0);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*infinite*DuckDBTimestamp*");
    }

    [Fact]
    public void ReadInfinityTimestampTzAsDateTimeOffset_ThrowsException()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMPTZ";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var act = () => reader.GetFieldValue<DateTimeOffset>(0);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*infinite*DuckDBTimestamp*");
    }

    [Fact]
    public void ReadInfinityTimestampVariantsAsDateTime_ThrowsException()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP_NS, 'infinity'::TIMESTAMP_MS, 'infinity'::TIMESTAMP_S";
        using var reader = Command.ExecuteReader();
        reader.Read();

        var act0 = () => reader.GetDateTime(0);
        act0.Should().Throw<InvalidOperationException>();

        var act1 = () => reader.GetDateTime(1);
        act1.Should().Throw<InvalidOperationException>();

        var act2 = () => reader.GetDateTime(2);
        act2.Should().Throw<InvalidOperationException>();
    }

    #endregion

    #region Mixed Infinity and Finite Values

    [Fact]
    public void ReadMixedInfinityDatesAsDuckDBDateOnly()
    {
        Command.CommandText = "SELECT * FROM (VALUES ('infinity'::DATE), ('-infinity'::DATE), ('2024-01-15'::DATE)) AS t(d)";
        using var reader = Command.ExecuteReader();

        reader.Read();
        var positiveInfinity = reader.GetFieldValue<DuckDBDateOnly>(0);
        positiveInfinity.IsPositiveInfinity.Should().BeTrue();

        reader.Read();
        var negativeInfinity = reader.GetFieldValue<DuckDBDateOnly>(0);
        negativeInfinity.IsNegativeInfinity.Should().BeTrue();

        reader.Read();
        var finiteDate = reader.GetFieldValue<DuckDBDateOnly>(0);
        finiteDate.IsInfinity.Should().BeFalse();
        finiteDate.Year.Should().Be(2024);
        finiteDate.Month.Should().Be(1);
        finiteDate.Day.Should().Be(15);
    }

    [Fact]
    public void ReadMixedInfinityTimestampsAsDuckDBTimestamp()
    {
        Command.CommandText = "SELECT * FROM (VALUES ('infinity'::TIMESTAMP), ('-infinity'::TIMESTAMP), ('2024-01-15 12:30:45'::TIMESTAMP)) AS t(ts)";
        using var reader = Command.ExecuteReader();

        reader.Read();
        var positiveInfinity = reader.GetFieldValue<DuckDBTimestamp>(0);
        positiveInfinity.IsPositiveInfinity.Should().BeTrue();

        reader.Read();
        var negativeInfinity = reader.GetFieldValue<DuckDBTimestamp>(0);
        negativeInfinity.IsNegativeInfinity.Should().BeTrue();

        reader.Read();
        var finiteTimestamp = reader.GetFieldValue<DuckDBTimestamp>(0);
        finiteTimestamp.IsInfinity.Should().BeFalse();
        finiteTimestamp.Date.Year.Should().Be(2024);
        finiteTimestamp.Date.Month.Should().Be(1);
        finiteTimestamp.Date.Day.Should().Be(15);
    }
}
