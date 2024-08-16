using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using Bogus;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class IntegerParametersTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
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

    private void TestSimple<TValue>(string duckDbType, TValue? expectedValue, Func<DuckDBDataReader, TValue?> getValue) where TValue : struct, INumberBase<TValue>
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

        void TestReadValueAs<T>(DuckDBDataReader reader) where T : INumberBase<T>
        {
            try
            {
                var convertedExpectedValue = expectedValue.HasValue ? T.CreateChecked(expectedValue.Value) : default;
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
        TestSimple<byte>("UTINYINT", value, r => r.GetByte(0));
        TestBind(value, new DuckDBParameter(value), r => r.GetByte(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(sbyte.MinValue)]
    [InlineData(sbyte.MaxValue)]
    public void SByteTest(sbyte value)
    {
        TestSimple<sbyte>("TINYINT", value, r => r.GetFieldValue<sbyte>(0));
        TestBind(value, new DuckDBParameter(value), r => r.GetFieldValue<sbyte>(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(ushort.MinValue)]
    [InlineData(ushort.MaxValue)]
    public void UInt16Test(ushort value)
    {
        TestSimple<ushort>("USMALLINT", value, r => r.GetFieldValue<ushort>(0));
        TestBind(value, new DuckDBParameter(value), r => r.GetFieldValue<ushort>(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(short.MinValue)]
    [InlineData(short.MaxValue)]
    public void Int16Test(short value)
    {
        TestSimple<short>("SMALLINT", value, r => r.GetInt16(0));
        TestBind(value, new DuckDBParameter(value), r => r.GetInt16(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(uint.MinValue)]
    [InlineData(uint.MaxValue)]
    public void UInt32Test(uint value)
    {
        TestSimple<uint>("UINTEGER", value, r => r.GetFieldValue<uint>(0));
        TestBind(value, new DuckDBParameter(value), r => r.GetFieldValue<uint>(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(int.MinValue)]
    [InlineData(int.MaxValue)]
    public void Int32Test(int value)
    {
        TestSimple<int>("INTEGER", value, r => r.GetInt32(0));
        TestBind(value, new DuckDBParameter(value), r => r.GetInt32(0));

        TestSimple<int>("INTEGER", value, r => r.GetFieldValue<int?>(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(ulong.MinValue)]
    [InlineData(ulong.MaxValue)]
    public void UInt64Test(ulong value)
    {
        TestSimple<ulong>("UBIGINT", value, r => r.GetFieldValue<ulong>(0));
        TestBind(value, new DuckDBParameter(value), r => r.GetFieldValue<ulong>(0));

        TestSimple("UBIGINT", value, r => r.GetFieldValue<ulong?>(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    public void Int64Test(long value)
    {
        TestSimple<long>("BIGINT", value, r => r.GetInt64(0));
        TestBind(value, new DuckDBParameter(value), r => r.GetInt64(0));

        TestSimple("BIGINT", value, r => r.GetFieldValue<long?>(0));
    }

    [Theory]
    [MemberData(nameof(GetBigIntegers))]
    public void VarintTest(BigInteger expectedValue)
    {
        TestSimple<BigInteger>("VARINT", expectedValue, r => r.GetFieldValue<BigInteger>(0));
    }

    public static IEnumerable<object[]> GetBigIntegers()
    {
        for (int i = 0; i < 1024 * 1 + 10; i++)
        {
            yield return new object[] { new BigInteger(i) };
            yield return new object[] { new BigInteger(-i) };

            yield return new object[] { new BigInteger(int.MaxValue - i) };
            yield return new object[] { new BigInteger(int.MaxValue + i) };

            yield return new object[] { new BigInteger(int.MinValue + i) };
            yield return new object[] { new BigInteger(int.MinValue - i) };

            yield return new object[] { new BigInteger(long.MaxValue - i) };
            yield return new object[] { new BigInteger(long.MaxValue + i) };

            yield return new object[] { new BigInteger(long.MinValue + i) };
            yield return new object[] { new BigInteger(long.MinValue - i) };
        }

        var faker = new Faker();
        var left = Enumerable.Range(0, 50).Select(i => faker.Random.Long(long.MaxValue - 100)).ToList();
        var right = Enumerable.Range(0, 50).Select(i => faker.Random.Long(long.MaxValue - 100)).ToList();

        foreach (var bigInteger in left.Zip(right, (l, r) => new BigInteger(l) * new BigInteger(r)))
        {
            yield return new object[] { bigInteger };
        }
    }
}