using DuckDB.NET.Data;
using DuckDB.NET.Native;
using FluentAssertions;
using System;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBInfinityTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    #region Native IsFinite Tests

    [Fact]
    public void duckdb_date_is_finite()
    {
        var positiveInfinity = DuckDBDate.PositiveInfinity;
        var negativeInfinity = DuckDBDate.NegativeInfinity;
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
        var positiveInfinity = DuckDBTimestampStruct.PositiveInfinity;
        var negativeInfinity = DuckDBTimestampStruct.NegativeInfinity;
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
        var positiveInfinity = DuckDBTimestampStruct.PositiveInfinity;
        var negativeInfinity = DuckDBTimestampStruct.NegativeInfinity;
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
        var positiveInfinity = DuckDBTimestampStruct.PositiveInfinity;
        var negativeInfinity = DuckDBTimestampStruct.NegativeInfinity;
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
        var positiveInfinity = DuckDBTimestampStruct.PositiveInfinity;
        var negativeInfinity = DuckDBTimestampStruct.NegativeInfinity;
        var finitePositive = new DuckDBTimestampStruct { Micros = Int64.MaxValue - 1 };
        var finiteNegative = new DuckDBTimestampStruct { Micros = -Int64.MaxValue + 1 };
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampNs(finitePositive).Should().BeTrue();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampNs(positiveInfinity).Should().BeFalse();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampNs(finiteNegative).Should().BeTrue();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampNs(negativeInfinity).Should().BeFalse();
    }

    #endregion

    #region Infinity Date Tests

    [Fact]
    public void ReadInfinityDate()
    {
        Command.CommandText = "SELECT 'infinity'::DATE, '-infinity'::DATE";
        using var reader = Command.ExecuteReader();
        reader.Read();

        // Positive infinity
        var positiveInfinity = reader.GetFieldValue<DuckDBDateOnly>(0);
        positiveInfinity.Should().Be(DuckDBDateOnly.PositiveInfinity);
        positiveInfinity.IsPositiveInfinity.Should().BeTrue();
        positiveInfinity.IsInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBDateOnly>(0).Should().Be(DuckDBDateOnly.PositiveInfinity);

        // Negative infinity
        var negativeInfinity = reader.GetFieldValue<DuckDBDateOnly>(1);
        negativeInfinity.Should().Be(DuckDBDateOnly.NegativeInfinity);
        negativeInfinity.IsNegativeInfinity.Should().BeTrue();
        negativeInfinity.IsInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBDateOnly>(1).Should().Be(DuckDBDateOnly.NegativeInfinity);

        // Reading as DateTime throws
        var actPositive = () => reader.GetDateTime(0);
        actPositive.Should().Throw<InvalidOperationException>().WithMessage("*infinite*DuckDBDateOnly*");
        var actNegative = () => reader.GetDateTime(1);
        actNegative.Should().Throw<InvalidOperationException>().WithMessage("*infinite*DuckDBDateOnly*");

        // Reading as nullable DateTime throws
        var actNullablePositive = () => reader.GetFieldValue<DateTime?>(0);
        actNullablePositive.Should().Throw<InvalidOperationException>().WithMessage("*infinite*DuckDBDateOnly*");

#if NET6_0_OR_GREATER
        // Reading as DateOnly throws
        var actDateOnlyPositive = () => reader.GetFieldValue<DateOnly>(0);
        actDateOnlyPositive.Should().Throw<InvalidOperationException>().WithMessage("*infinite*DuckDBDateOnly*");
        var actDateOnlyNegative = () => reader.GetFieldValue<DateOnly>(1);
        actDateOnlyNegative.Should().Throw<InvalidOperationException>().WithMessage("*infinite*DuckDBDateOnly*");

        // Reading as nullable DateOnly throws
        var actNullableDateOnly = () => reader.GetFieldValue<DateOnly?>(0);
        actNullableDateOnly.Should().Throw<InvalidOperationException>().WithMessage("*infinite*DuckDBDateOnly*");
#endif
    }

    #endregion

    #region Infinity Timestamp Tests

    [Fact]
    public void ReadInfinityTimestamp()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP, '-infinity'::TIMESTAMP";
        using var reader = Command.ExecuteReader();
        reader.Read();

        // Positive infinity
        var positiveInfinity = reader.GetFieldValue<DuckDBTimestamp>(0);
        positiveInfinity.Should().Be(DuckDBTimestamp.PositiveInfinity);
        positiveInfinity.IsPositiveInfinity.Should().BeTrue();
        positiveInfinity.IsInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBTimestamp>(0).Should().Be(DuckDBTimestamp.PositiveInfinity);

        // Negative infinity
        var negativeInfinity = reader.GetFieldValue<DuckDBTimestamp>(1);
        negativeInfinity.Should().Be(DuckDBTimestamp.NegativeInfinity);
        negativeInfinity.IsNegativeInfinity.Should().BeTrue();
        negativeInfinity.IsInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBTimestamp>(1).Should().Be(DuckDBTimestamp.NegativeInfinity);

        // Reading as DateTime throws
        var actPositive = () => reader.GetDateTime(0);
        actPositive.Should().Throw<InvalidOperationException>().WithMessage("*infinite*DuckDBTimestamp*");
        var actNegative = () => reader.GetDateTime(1);
        actNegative.Should().Throw<InvalidOperationException>().WithMessage("*infinite*DuckDBTimestamp*");

        // Reading as nullable DateTime throws
        var actNullable = () => reader.GetFieldValue<DateTime?>(0);
        actNullable.Should().Throw<InvalidOperationException>().WithMessage("*infinite*DuckDBTimestamp*");

        // Reading as DateTimeOffset throws
        var actOffsetPositive = () => reader.GetFieldValue<DateTimeOffset>(0);
        actOffsetPositive.Should().Throw<InvalidOperationException>().WithMessage("*infinite*DuckDBTimestamp*");
        var actOffsetNegative = () => reader.GetFieldValue<DateTimeOffset>(1);
        actOffsetNegative.Should().Throw<InvalidOperationException>().WithMessage("*infinite*DuckDBTimestamp*");

        // Reading as nullable DateTimeOffset throws
        var actNullableOffset = () => reader.GetFieldValue<DateTimeOffset?>(0);
        actNullableOffset.Should().Throw<InvalidOperationException>().WithMessage("*infinite*DuckDBTimestamp*");
    }

    [Fact]
    public void ReadInfinityTimestampNs()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP_NS, '-infinity'::TIMESTAMP_NS";
        using var reader = Command.ExecuteReader();
        reader.Read();

        // Positive infinity
        var positiveInfinity = reader.GetFieldValue<DuckDBTimestamp>(0);
        positiveInfinity.IsPositiveInfinity.Should().BeTrue();
        positiveInfinity.IsInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBTimestamp>(0).IsPositiveInfinity.Should().BeTrue();

        // Negative infinity
        var negativeInfinity = reader.GetFieldValue<DuckDBTimestamp>(1);
        negativeInfinity.IsNegativeInfinity.Should().BeTrue();
        negativeInfinity.IsInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBTimestamp>(1).IsNegativeInfinity.Should().BeTrue();

        // Reading as DateTime throws
        var actPositive = () => reader.GetDateTime(0);
        actPositive.Should().Throw<InvalidOperationException>();
        var actNegative = () => reader.GetDateTime(1);
        actNegative.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ReadInfinityTimestampMs()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP_MS, '-infinity'::TIMESTAMP_MS";
        using var reader = Command.ExecuteReader();
        reader.Read();

        // Positive infinity
        var positiveInfinity = reader.GetFieldValue<DuckDBTimestamp>(0);
        positiveInfinity.IsPositiveInfinity.Should().BeTrue();
        positiveInfinity.IsInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBTimestamp>(0).IsPositiveInfinity.Should().BeTrue();

        // Negative infinity
        var negativeInfinity = reader.GetFieldValue<DuckDBTimestamp>(1);
        negativeInfinity.IsNegativeInfinity.Should().BeTrue();
        negativeInfinity.IsInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBTimestamp>(1).IsNegativeInfinity.Should().BeTrue();

        // Reading as DateTime throws
        var actPositive = () => reader.GetDateTime(0);
        actPositive.Should().Throw<InvalidOperationException>();
        var actNegative = () => reader.GetDateTime(1);
        actNegative.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ReadInfinityTimestampS()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP_S, '-infinity'::TIMESTAMP_S";
        using var reader = Command.ExecuteReader();
        reader.Read();

        // Positive infinity
        var positiveInfinity = reader.GetFieldValue<DuckDBTimestamp>(0);
        positiveInfinity.IsPositiveInfinity.Should().BeTrue();
        positiveInfinity.IsInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBTimestamp>(0).IsPositiveInfinity.Should().BeTrue();

        // Negative infinity
        var negativeInfinity = reader.GetFieldValue<DuckDBTimestamp>(1);
        negativeInfinity.IsNegativeInfinity.Should().BeTrue();
        negativeInfinity.IsInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBTimestamp>(1).IsNegativeInfinity.Should().BeTrue();

        // Reading as DateTime throws
        var actPositive = () => reader.GetDateTime(0);
        actPositive.Should().Throw<InvalidOperationException>();
        var actNegative = () => reader.GetDateTime(1);
        actNegative.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ReadInfinityTimestampTz()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMPTZ, '-infinity'::TIMESTAMPTZ";
        using var reader = Command.ExecuteReader();
        reader.Read();

        // Positive infinity
        var positiveInfinity = reader.GetFieldValue<DuckDBTimestamp>(0);
        positiveInfinity.IsPositiveInfinity.Should().BeTrue();
        positiveInfinity.IsInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBTimestamp>(0).IsPositiveInfinity.Should().BeTrue();

        // Negative infinity
        var negativeInfinity = reader.GetFieldValue<DuckDBTimestamp>(1);
        negativeInfinity.IsNegativeInfinity.Should().BeTrue();
        negativeInfinity.IsInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBTimestamp>(1).IsNegativeInfinity.Should().BeTrue();

        // Reading as DateTime throws
        var actPositive = () => reader.GetDateTime(0);
        actPositive.Should().Throw<InvalidOperationException>().WithMessage("*infinite*DuckDBTimestamp*");
        var actNegative = () => reader.GetDateTime(1);
        actNegative.Should().Throw<InvalidOperationException>().WithMessage("*infinite*DuckDBTimestamp*");

        // Reading as DateTimeOffset throws
        var actOffsetPositive = () => reader.GetFieldValue<DateTimeOffset>(0);
        actOffsetPositive.Should().Throw<InvalidOperationException>().WithMessage("*infinite*DuckDBTimestamp*");
        var actOffsetNegative = () => reader.GetFieldValue<DateTimeOffset>(1);
        actOffsetNegative.Should().Throw<InvalidOperationException>().WithMessage("*infinite*DuckDBTimestamp*");
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

    #endregion
}
