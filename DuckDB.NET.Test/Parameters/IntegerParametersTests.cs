namespace DuckDB.NET.Test.Parameters;

public class IntegerParametersTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    private void TestBind<TValue>(string duckDbType, TValue expectedValue, DuckDBParameter parameter, Func<DuckDBDataReader, TValue> getValue)
    {
        Command.CommandText = $"SELECT ?::{duckDbType};";
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

            using var reader = Command.ExecuteReader();
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
        TestBind("UTINYINT", value, new DuckDBParameter(value), r => r.GetByte(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(sbyte.MinValue)]
    [InlineData(sbyte.MaxValue)]
    public void SByteTest(sbyte value)
    {
        TestSimple<sbyte>("TINYINT", value, r => r.GetFieldValue<sbyte>(0));
        TestBind("TINYINT", value, new DuckDBParameter(value), r => r.GetFieldValue<sbyte>(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(ushort.MinValue)]
    [InlineData(ushort.MaxValue)]
    public void UInt16Test(ushort value)
    {
        TestSimple<ushort>("USMALLINT", value, r => r.GetFieldValue<ushort>(0));
        TestBind("USMALLINT", value, new DuckDBParameter(value), r => r.GetFieldValue<ushort>(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(short.MinValue)]
    [InlineData(short.MaxValue)]
    public void Int16Test(short value)
    {
        TestSimple<short>("SMALLINT", value, r => r.GetInt16(0));
        TestBind("SMALLINT", value, new DuckDBParameter(value), r => r.GetInt16(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(uint.MinValue)]
    [InlineData(uint.MaxValue)]
    public void UInt32Test(uint value)
    {
        TestSimple<uint>("UINTEGER", value, r => r.GetFieldValue<uint>(0));
        TestBind("UINTEGER", value, new DuckDBParameter(value), r => r.GetFieldValue<uint>(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(int.MinValue)]
    [InlineData(int.MaxValue)]
    public void Int32Test(int value)
    {
        TestSimple<int>("INTEGER", value, r => r.GetInt32(0));
        TestBind("INTEGER", value, new DuckDBParameter(value), r => r.GetInt32(0));

        TestSimple<int>("INTEGER", value, r => r.GetFieldValue<int?>(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(ulong.MinValue)]
    [InlineData(ulong.MaxValue)]
    public void UInt64Test(ulong value)
    {
        TestSimple<ulong>("UBIGINT", value, r => r.GetFieldValue<ulong>(0));
        TestBind("UBIGINT", value, new DuckDBParameter(value), r => r.GetFieldValue<ulong>(0));

        TestSimple("UBIGINT", value, r => r.GetFieldValue<ulong?>(0));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    public void Int64Test(long value)
    {
        TestSimple<long>("BIGINT", value, r => r.GetInt64(0));
        TestBind("BIGINT", value, new DuckDBParameter(value), r => r.GetInt64(0));

        TestSimple("BIGINT", value, r => r.GetFieldValue<long?>(0));
    }

    [Theory]
    [Trait("Category", "Long Running")]
    [MemberData(nameof(GetBigIntegers))]
    public void VarintTest(BigInteger expectedValue)
    {
        TestSimple<BigInteger>("VARINT", expectedValue, r => r.GetFieldValue<BigInteger>(0));
    }

    [Theory]
    [InlineData("170141183460469231731687303715884105728")]    // 2^127, one past HUGEINT
    [InlineData("1361129467683753853853498429727072845824")]   // 2^130
    [InlineData("-1361129467683753853853498429727072845824")]
    public void VarintParameterBeyondHugeIntTest(string value)
    {
        var expectedValue = BigInteger.Parse(value);

        Command.CommandText = "CREATE TABLE varint_parameter_test (a VARINT);";
        Command.ExecuteNonQuery();

        try
        {
            Command.CommandText = "INSERT INTO varint_parameter_test (a) VALUES (?);";
            Command.Parameters.Add(new DuckDBParameter(expectedValue));
            Command.ExecuteNonQuery();
            Command.Parameters.Clear();

            Command.CommandText = "SELECT a FROM varint_parameter_test;";
            Command.ExecuteScalar().Should().Be(expectedValue);
        }
        finally
        {
            Command.Parameters.Clear();
            Command.CommandText = "DROP TABLE varint_parameter_test;";
            Command.ExecuteNonQuery();
        }
    }

    [Theory]
    [InlineData("72057594037927936")]                          // 2^56
    [InlineData("72057594037927937")]
    [InlineData("-72057594037927936")]
    [InlineData("1329227995784915872903807060280344576")]      // 2^120
    [InlineData("-1329227995784915872903807060280344577")]
    public void VarintParameterAroundDuckDBCastBugTest(string value)
    {
        // DuckDB drops the top byte when it casts an integer to VARINT around 2^56 and 2^120
        // (SELECT 72057594037927936::BIGINT::VARINT returns 0). Binding the value as a VARINT
        // avoids that cast, so these must survive even on engines that still have the bug.
        VarintParameterBeyondHugeIntTest(value);
    }

    public static IEnumerable<object[]> GetBigIntegers()
    {
        // A set, because xUnit skips theory rows with duplicate arguments
        var values = new HashSet<BigInteger>
        {
            // https://github.com/duckdb/duckdb/issues/13239
            BigInteger.Parse("85070591730234614260976917445211069672")
        };

        // Both sides of the int and long limits. The arithmetic is done in BigInteger so it does not wrap around.
        for (int i = 0; i < 64; i++)
        {
            values.Add(i);
            values.Add(-i);

            foreach (BigInteger limit in new long[] { int.MaxValue, int.MinValue, long.MaxValue, long.MinValue })
            {
                values.Add(limit - i);
                values.Add(limit + i);
            }
        }

        // Every point where the value needs one more bit, which includes every point where a VARINT needs one more byte
        for (int bits = 0; bits <= 256; bits++)
        {
            var power = BigInteger.Pow(2, bits);

            foreach (var value in new[] { power - 1, power, power + 1 })
            {
                values.Add(value);
                values.Add(-value);
            }
        }

        var faker = new Faker();
        var left = Enumerable.Range(0, 50).Select(i => faker.Random.Long(long.MaxValue - 100)).ToList();
        var right = Enumerable.Range(0, 50).Select(i => faker.Random.Long(long.MaxValue - 100)).ToList();

        values.UnionWith(left.Zip(right, (l, r) => new BigInteger(l) * new BigInteger(r)));

        return values.Select(value => new object[] { value });
    }

    [Fact]
    public void BindParameterWithoutTable_Int32()
    {
        var value = Faker.Random.Int();
        
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));
        
        var result = Command.ExecuteScalar();
        
        result.Should().BeOfType<int>().Subject
              .Should().Be(value);
    }

    [Fact]
    public void BindParameterWithoutTable_Int64()
    {
        var value = Faker.Random.Long();
        
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));
        
        var result = Command.ExecuteScalar();
        
        result.Should().BeOfType<long>().Subject
              .Should().Be(value);
    }

    [Fact]
    public void BindParameterWithoutTable_Int16()
    {
        var value = Faker.Random.Short();
        
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));
        
        var result = Command.ExecuteScalar();
        
        result.Should().BeOfType<short>().Subject
              .Should().Be(value);
    }

    [Fact]
    public void BindParameterWithoutTable_SByte()
    {
        var value = Faker.Random.SByte();
        
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));
        
        var result = Command.ExecuteScalar();
        
        result.Should().BeOfType<sbyte>().Subject
              .Should().Be(value);
    }

    [Fact]
    public void BindParameterWithoutTable_Byte()
    {
        var value = Faker.Random.Byte();
        
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));
        
        var result = Command.ExecuteScalar();
        
        result.Should().BeOfType<byte>().Subject
              .Should().Be(value);
    }

    [Fact]
    public void BindParameterWithoutTable_UInt16()
    {
        var value = Faker.Random.UShort();
        
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));
        
        var result = Command.ExecuteScalar();
        
        result.Should().BeOfType<ushort>().Subject
              .Should().Be(value);
    }

    [Fact]
    public void BindParameterWithoutTable_UInt32()
    {
        var value = Faker.Random.UInt();
        
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));
        
        var result = Command.ExecuteScalar();
        
        result.Should().BeOfType<uint>().Subject
              .Should().Be(value);
    }

    [Fact]
    public void BindParameterWithoutTable_UInt64()
    {
        var value = Faker.Random.ULong();
        
        Command.CommandText = "SELECT ?;";
        Command.Parameters.Add(new DuckDBParameter(value));
        
        var result = Command.ExecuteScalar();
        
        result.Should().BeOfType<ulong>().Subject
              .Should().Be(value);
    }
}