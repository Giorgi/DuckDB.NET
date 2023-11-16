using System;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class IntegerParametersTests : DuckDBTestBase
{
    public IntegerParametersTests(DuckDBDatabaseFixture db) : base(db)
    {
    }

    private void TestBind<TValue>(TValue expectedValue, DuckDBParameter parameter, Func<DuckDBDataReader, TValue> getValue)
    {
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(parameter);

        var scalar = Command.ExecuteScalar();
        scalar.Should().Be(expectedValue);

        var reader = Command.ExecuteReader();
        reader.Read();
        var value = getValue(reader);

        value.Should().Be(expectedValue);
    }

    private void TestSimple<TValue>(string duckDbType, TValue expectedValue, Func<DuckDBDataReader, TValue> getValue)
    {
        Command.CommandText = $"CREATE TABLE {duckDbType}_test (a {duckDbType});";
        Command.ExecuteNonQuery();

        try
        {
            Command.CommandText = $"INSERT INTO {duckDbType}_test (a) VALUES ({expectedValue});";
            Command.ExecuteNonQuery();

            Command.CommandText = $"SELECT * FROM {duckDbType}_test;";

            var scalar = Command.ExecuteScalar();
            scalar.Should().Be(expectedValue);

            var reader = Command.ExecuteReader();
            reader.Read();
            var value = getValue(reader);

            value.Should().Be(expectedValue);

            reader.Invoking(r => r.GetFieldValue<string>(0)).Should().Throw<InvalidCastException>();
            reader.GetFieldType(0).Should().Match(type => type == typeof(TValue) || type == Nullable.GetUnderlyingType(typeof(TValue)));

            TestReadValueAs<byte>(reader);
            TestReadValueAs<sbyte>(reader);
            TestReadValueAs<ushort>(reader);
            TestReadValueAs<short>(reader);
            TestReadValueAs<uint>(reader);
            TestReadValueAs<int>(reader);
            TestReadValueAs<ulong>(reader);
            TestReadValueAs<long>(reader);
        }
        finally
        {
            Command.CommandText = $"DROP TABLE {duckDbType}_test;";
            Command.ExecuteNonQuery();
        }

        void TestReadValueAs<T>(DuckDBDataReader reader)
        {
            try
            {
                var convertedExpectedValue = (T)Convert.ChangeType(expectedValue, typeof(T));
                convertedExpectedValue.Should().Be(reader.GetFieldValue<T>(0));
            }
            catch (Exception)
            {
                reader.Invoking(dataReader => dataReader.GetFieldValue<T>(0)).Should().Throw<InvalidCastException>();
            }
        }
    }

    [Theory]
    [InlineData(0)]
    [InlineData(byte.MinValue)]
    [InlineData(byte.MaxValue)]
    public void ByteTest(byte value)
    {
        TestSimple("UTINYINT", value, r => r.GetByte(0));
        TestBind(value, new DuckDBParameter(value), r => r.GetByte(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(sbyte.MinValue)]
    [InlineData(sbyte.MaxValue)]
    public void SByteTest(sbyte value)
    {
        TestSimple("TINYINT", value, r => r.GetFieldValue<sbyte>(0));
        TestBind(value, new DuckDBParameter(value), r => r.GetFieldValue<sbyte>(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(ushort.MinValue)]
    [InlineData(ushort.MaxValue)]
    public void UInt16Test(ushort value)
    {
        TestSimple("USMALLINT", value, r => r.GetFieldValue<ushort>(0));
        TestBind(value, new DuckDBParameter(value), r => r.GetFieldValue<ushort>(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(short.MinValue)]
    [InlineData(short.MaxValue)]
    public void Int16Test(short value)
    {
        TestSimple("SMALLINT", value, r => r.GetInt16(0));
        TestBind(value, new DuckDBParameter(value), r => r.GetInt16(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(uint.MinValue)]
    [InlineData(uint.MaxValue)]
    public void UInt32Test(uint value)
    {
        TestSimple("UINTEGER", value, r => r.GetFieldValue<uint>(0));
        TestBind(value, new DuckDBParameter(value), r => r.GetFieldValue<uint>(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(int.MinValue)]
    [InlineData(int.MaxValue)]
    public void Int32Test(int value)
    {
        TestSimple("INTEGER", value, r => r.GetInt32(0));
        TestBind(value, new DuckDBParameter(value), r => r.GetInt32(0));

        TestSimple("INTEGER", value, r => r.GetFieldValue<int?>(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(ulong.MinValue)]
    [InlineData(ulong.MaxValue)]
    public void UInt64Test(ulong value)
    {
        TestSimple("UBIGINT", value, r => r.GetFieldValue<ulong>(0));
        TestBind(value, new DuckDBParameter(value), r => r.GetFieldValue<ulong>(0));

        TestSimple("UBIGINT", value, r => r.GetFieldValue<ulong?>(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    public void Int64Test(long value)
    {
        TestSimple("BIGINT", value, r => r.GetInt64(0));
        TestBind(value, new DuckDBParameter(value), r => r.GetInt64(0));

        TestSimple("BIGINT", value, r => r.GetFieldValue<long?>(0));
    }
}