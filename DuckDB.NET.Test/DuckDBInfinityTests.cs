using DuckDB.NET.Data;
using DuckDB.NET.Native;
using FluentAssertions;
using System;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBInfinityTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    #region Assertion Helpers

    private static void AssertInfinityDateValues(DuckDBDataReader reader)
    {
        // Positive infinity
        var positiveInfinity = reader.GetFieldValue<DuckDBDateOnly>(0);
        positiveInfinity.Should().Be(DuckDBDateOnly.PositiveInfinity);
        positiveInfinity.IsPositiveInfinity.Should().BeTrue();
        positiveInfinity.IsInfinity.Should().BeTrue();
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteDate(positiveInfinity.ToDuckDBDate()).Should().BeFalse();

        // Negative infinity
        var negativeInfinity = reader.GetFieldValue<DuckDBDateOnly>(1);
        negativeInfinity.Should().Be(DuckDBDateOnly.NegativeInfinity);
        negativeInfinity.IsNegativeInfinity.Should().BeTrue();
        negativeInfinity.IsInfinity.Should().BeTrue();
        reader.GetFieldValue<DuckDBDateOnly>(1).Should().Be(DuckDBDateOnly.NegativeInfinity);
        NativeMethods.DateTimeHelpers.DuckDBIsFiniteDate(negativeInfinity.ToDuckDBDate()).Should().BeFalse();

        // isinf() function results
        reader.GetBoolean(2).Should().BeTrue("isinf() should return true for positive infinity date");
        reader.GetBoolean(3).Should().BeTrue("isinf() should return true for negative infinity date");

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

    private static void AssertInfinityTimestampValues(DuckDBDataReader reader, DuckDBType duckDBType)
    {
        bool IsFinite(DuckDBTimestampStruct timestamp)
        {
            return duckDBType switch
            {
                DuckDBType.TimestampNs => NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampNs(timestamp),
                DuckDBType.TimestampMs => NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampMs(timestamp),
                DuckDBType.TimestampS => NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestampS(timestamp),
                _ => NativeMethods.DateTimeHelpers.DuckDBIsFiniteTimestamp(timestamp)
            };
        }

        // Positive infinity
        var positiveInfinity = reader.GetFieldValue<DuckDBTimestamp>(0);
        positiveInfinity.Should().Be(DuckDBTimestamp.PositiveInfinity);
        positiveInfinity.IsPositiveInfinity.Should().BeTrue();
        positiveInfinity.IsInfinity.Should().BeTrue();
        IsFinite(positiveInfinity.ToDuckDBTimestampStruct()).Should().BeFalse();

        // Negative infinity
        var negativeInfinity = reader.GetFieldValue<DuckDBTimestamp>(1);
        negativeInfinity.Should().Be(DuckDBTimestamp.NegativeInfinity);
        negativeInfinity.IsNegativeInfinity.Should().BeTrue();
        negativeInfinity.IsInfinity.Should().BeTrue();
        IsFinite(negativeInfinity.ToDuckDBTimestampStruct()).Should().BeFalse();

        // isinf() function results
        reader.GetBoolean(2).Should().BeTrue("isinf() should return true for positive infinity timestamp");
        reader.GetBoolean(3).Should().BeTrue("isinf() should return true for negative infinity timestamp");

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

    #endregion

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
        Command.CommandText = "SELECT 'infinity'::DATE, '-infinity'::DATE, isinf('infinity'::DATE), isinf('-infinity'::DATE)";
        using var reader = Command.ExecuteReader();
        reader.Read();

        AssertInfinityDateValues(reader);
    }

    [Fact]
    public void ReadInfinityDateWithParameters()
    {
        Command.CommandText = "SELECT $1::DATE, $2::DATE, isinf($1::DATE), isinf($2::DATE)";
        Command.Parameters.Add(new DuckDBParameter(DuckDBDateOnly.PositiveInfinity));
        Command.Parameters.Add(new DuckDBParameter(DuckDBDateOnly.NegativeInfinity));
        using var reader = Command.ExecuteReader();
        reader.Read();

        AssertInfinityDateValues(reader);
    }

    #endregion

    #region Infinity Timestamp Tests

    [Fact]
    public void ReadInfinityTimestamp()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP, '-infinity'::TIMESTAMP, isinf('infinity'::TIMESTAMP), isinf('-infinity'::TIMESTAMP)";
        using var reader = Command.ExecuteReader();
        reader.Read();

        AssertInfinityTimestampValues(reader, DuckDBType.Timestamp);
    }

    [Fact]
    public void ReadInfinityTimestampWithParameters()
    {
        Command.CommandText = "SELECT $1::TIMESTAMP, $2::TIMESTAMP, isinf($1::TIMESTAMP), isinf($2::TIMESTAMP)";
        Command.Parameters.Add(new DuckDBParameter(DuckDBTimestamp.PositiveInfinity));
        Command.Parameters.Add(new DuckDBParameter(DuckDBTimestamp.NegativeInfinity));
        using var reader = Command.ExecuteReader();
        reader.Read();

        AssertInfinityTimestampValues(reader, DuckDBType.Timestamp);
    }

    [Fact]
    public void ReadInfinityTimestampNs()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP_NS, '-infinity'::TIMESTAMP_NS, isinf('infinity'::TIMESTAMP_NS), isinf('-infinity'::TIMESTAMP_NS)";
        using var reader = Command.ExecuteReader();
        reader.Read();

        AssertInfinityTimestampValues(reader, DuckDBType.TimestampNs);
    }

    [Fact]
    public void ReadInfinityTimestampNsWithParameters()
    {
        Command.CommandText = "SELECT $1::TIMESTAMP_NS, $2::TIMESTAMP_NS, isinf($1::TIMESTAMP_NS), isinf($2::TIMESTAMP_NS)";
        Command.Parameters.Add(new DuckDBParameter(DuckDBTimestamp.PositiveInfinity));
        Command.Parameters.Add(new DuckDBParameter(DuckDBTimestamp.NegativeInfinity));
        using var reader = Command.ExecuteReader();
        reader.Read();

        AssertInfinityTimestampValues(reader, DuckDBType.TimestampNs);
    }

    [Fact]
    public void ReadInfinityTimestampMs()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP_MS, '-infinity'::TIMESTAMP_MS, isinf('infinity'::TIMESTAMP_MS), isinf('-infinity'::TIMESTAMP_MS)";
        using var reader = Command.ExecuteReader();
        reader.Read();

        AssertInfinityTimestampValues(reader, DuckDBType.TimestampMs);
    }

    [Fact]
    public void ReadInfinityTimestampMsWithParameters()
    {
        Command.CommandText = "SELECT $1::TIMESTAMP_MS, $2::TIMESTAMP_MS, isinf($1::TIMESTAMP_MS), isinf($2::TIMESTAMP_MS)";
        Command.Parameters.Add(new DuckDBParameter(DuckDBTimestamp.PositiveInfinity));
        Command.Parameters.Add(new DuckDBParameter(DuckDBTimestamp.NegativeInfinity));
        using var reader = Command.ExecuteReader();
        reader.Read();

        AssertInfinityTimestampValues(reader, DuckDBType.TimestampMs);
    }

    [Fact]
    public void ReadInfinityTimestampS()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMP_S, '-infinity'::TIMESTAMP_S, isinf('infinity'::TIMESTAMP_S), isinf('-infinity'::TIMESTAMP_S)";
        using var reader = Command.ExecuteReader();
        reader.Read();

        AssertInfinityTimestampValues(reader, DuckDBType.TimestampS);
    }

    [Fact]
    public void ReadInfinityTimestampSWithParameters()
    {
        Command.CommandText = "SELECT $1::TIMESTAMP_S, $2::TIMESTAMP_S, isinf($1::TIMESTAMP_S), isinf($2::TIMESTAMP_S)";
        Command.Parameters.Add(new DuckDBParameter(DuckDBTimestamp.PositiveInfinity));
        Command.Parameters.Add(new DuckDBParameter(DuckDBTimestamp.NegativeInfinity));
        using var reader = Command.ExecuteReader();
        reader.Read();

        AssertInfinityTimestampValues(reader, DuckDBType.TimestampS);
    }

    [Fact]
    public void ReadInfinityTimestampTz()
    {
        Command.CommandText = "SELECT 'infinity'::TIMESTAMPTZ, '-infinity'::TIMESTAMPTZ, isinf('infinity'::TIMESTAMPTZ), isinf('-infinity'::TIMESTAMPTZ)";
        using var reader = Command.ExecuteReader();
        reader.Read();

        AssertInfinityTimestampValues(reader, DuckDBType.TimestampTz);
    }

    [Fact]
    public void ReadInfinityTimestampTzWithParameters()
    {
        Command.CommandText = "SELECT $1::TIMESTAMPTZ, $2::TIMESTAMPTZ, isinf($1::TIMESTAMPTZ), isinf($2::TIMESTAMPTZ)";
        Command.Parameters.Add(new DuckDBParameter(DuckDBTimestamp.PositiveInfinity));
        Command.Parameters.Add(new DuckDBParameter(DuckDBTimestamp.NegativeInfinity));
        using var reader = Command.ExecuteReader();
        reader.Read();

        AssertInfinityTimestampValues(reader, DuckDBType.TimestampTz);
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
