using System;
using DuckDB.NET.Data;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class IntegerParametersTests
{
    private static void TestBind<TValue>(DuckDBConnection connection, TValue expectedValue, DuckDBParameter parameter, Func<DuckDBDataReader, TValue> getValue)
    {
        var command = connection.CreateCommand();
        command.CommandText = "SELECT ?;";
        command.Parameters.Add(parameter);

        var scalar = command.ExecuteScalar();
        scalar.Should().Be(expectedValue);

        var reader = command.ExecuteReader();
        reader.Read();
        var value = getValue(reader);

        value.Should().Be(expectedValue);
    }
    
    private static void TestSimple<TValue>(DuckDBConnection connection, string duckDbType, TValue expectedValue, Func<DuckDBDataReader, TValue> getValue)
    {
        var command = connection.CreateCommand();
        command.CommandText = $"CREATE TABLE {duckDbType}_test (a {duckDbType});";
        command.ExecuteNonQuery();

        try
        {
            command.CommandText = $"INSERT INTO {duckDbType}_test (a) VALUES ({expectedValue});";
            command.ExecuteNonQuery();

            command.CommandText = $"SELECT * FROM {duckDbType}_test;";

            var scalar = command.ExecuteScalar();
            scalar.Should().Be(expectedValue);

            var reader = command.ExecuteReader();
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
            command.CommandText = $"DROP TABLE {duckDbType}_test;";
            command.ExecuteNonQuery();
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
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        
        TestSimple(connection, "UTINYINT", value, r => r.GetByte(0));
        TestBind(connection, value, new DuckDBParameter(value), r => r.GetByte(0));
    }
    
    [Theory]
    [InlineData(0)]
    [InlineData(sbyte.MinValue)]
    [InlineData(sbyte.MaxValue)]
    public void SByteTest(sbyte value)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        
        TestSimple(connection, "TINYINT", value, r => r.GetFieldValue<sbyte>(0));
        TestBind(connection, value, new DuckDBParameter(value), r => r.GetFieldValue<sbyte>(0));
    }
    
    [Theory]
    [InlineData(0)]
    [InlineData(ushort.MinValue)]
    [InlineData(ushort.MaxValue)]
    public void UInt16Test(ushort value)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        
        TestSimple(connection, "USMALLINT", value, r => r.GetFieldValue<ushort>(0));
        TestBind(connection, value, new DuckDBParameter(value), r => r.GetFieldValue<ushort>(0));
    }
    
    [Theory]
    [InlineData(0)]
    [InlineData(short.MinValue)]
    [InlineData(short.MaxValue)]
    public void Int16Test(short value)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        
        TestSimple(connection, "SMALLINT", value, r => r.GetInt16(0));
        TestBind(connection, value, new DuckDBParameter(value), r => r.GetInt16(0));
    }
    
    [Theory]
    [InlineData(0)]
    [InlineData(uint.MinValue)]
    [InlineData(uint.MaxValue)]
    public void UInt32Test(uint value)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        
        TestSimple(connection, "UINTEGER", value, r => r.GetFieldValue<uint>(0));
        TestBind(connection, value, new DuckDBParameter(value), r => r.GetFieldValue<uint>(0));
    }
    
    [Theory]
    [InlineData(0)]
    [InlineData(int.MinValue)]
    [InlineData(int.MaxValue)]
    public void Int32Test(int value)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        
        TestSimple(connection, "INTEGER", value, r => r.GetInt32(0));
        TestBind(connection, value, new DuckDBParameter(value), r => r.GetInt32(0));

        TestSimple(connection, "INTEGER", value, r => r.GetFieldValue<int?>(0));
    }
    
    [Theory]
    [InlineData(0)]
    [InlineData(ulong.MinValue)]
    [InlineData(ulong.MaxValue)]
    public void UInt64Test(ulong value)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        
        TestSimple(connection, "UBIGINT", value, r => r.GetFieldValue<ulong>(0));
        TestBind(connection, value, new DuckDBParameter(value), r => r.GetFieldValue<ulong>(0));

        TestSimple(connection, "UBIGINT", value, r => r.GetFieldValue<ulong?>(0));
    }
    
    [Theory]
    [InlineData(0)]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    public void Int64Test(long value)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        
        TestSimple(connection, "BIGINT", value, r => r.GetInt64(0));
        TestBind(connection, value, new DuckDBParameter(value), r => r.GetInt64(0));

        TestSimple(connection, "BIGINT", value, r => r.GetFieldValue<long?>(0));
    }
}