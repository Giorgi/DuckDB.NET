using Xunit;
using System;

namespace DuckDB.NET.Test;

public class DuckDBManagedAppenderTypeValidationTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void TryAppendFloatToDouble()
    {
        Command.CommandText = "create table appender_type_test_double (value double);";

        Command.ExecuteNonQuery();

        using var appender = Connection.CreateAppender("", "appender_type_test_double");
        {
            var dbRow = appender.CreateRow();

            Assert.Throws<InvalidOperationException>(() => dbRow.AppendValue((float)1).EndRow());
        }
    }

    [Fact]
    public void TryAppendDoubleToFloat()
    {
        Command.CommandText = "create table appender_type_test_real (value real);";

        Command.ExecuteNonQuery();

        using var appender = Connection.CreateAppender("", "appender_type_test_real");
        {
            var dbRow = appender.CreateRow();

            Assert.Throws<InvalidOperationException>(() => dbRow.AppendValue((double)1).EndRow());
        }
    }

    [Fact]
    public void TryAppendIntToUInt32()
    {
        Command.CommandText = "create table appender_type_test_uint (value uinteger);"; 

        Command.ExecuteNonQuery();

        using var appender = Connection.CreateAppender("", "appender_type_test_uint");
        {
            var dbRow = appender.CreateRow();

            Assert.Throws<InvalidOperationException>(() => dbRow.AppendValue(1).EndRow());
        }
    }

    [Fact]
    public void TryAppendIntToTinyInt()
    {
        Command.CommandText = "create table appender_type_test_tinyint (value tinyint);";

        Command.ExecuteNonQuery();

        using var appender = Connection.CreateAppender("", "appender_type_test_tinyint");
        {
            var dbRow = appender.CreateRow();

            // TinyInt expects sbyte, passing int should cause validation to fail
            Assert.Throws<InvalidOperationException>(() => dbRow.AppendValue(1).EndRow());
        }
    }

    [Fact]
    public void AppendShortToSmallint()
    {
        Command.CommandText = "create table appender_type_test_smallint (value smallint);";

        Command.ExecuteNonQuery();

        using var appender = Connection.CreateAppender("", "appender_type_test_smallint");
        {
            var dbRow = appender.CreateRow();

            dbRow.AppendValue((short)1).EndRow();
        }
    }

    [Fact]
    public void AppendNullableShortToSmallint()
    {
        Command.CommandText = "create table appender_type_test_smallint_nullable (value smallint);";

        Command.ExecuteNonQuery();

        using var appender = Connection.CreateAppender("", "appender_type_test_smallint_nullable");
        {
            var dbRow = appender.CreateRow();

            dbRow.AppendValue((short?)null).EndRow();
        }
    }

    [Fact]
    public void TryAppendLongToInteger()
    {
        Command.CommandText = "create table appender_type_test_integer (value integer);";

        Command.ExecuteNonQuery();

        using var appender = Connection.CreateAppender("", "appender_type_test_integer");
        {
            var dbRow = appender.CreateRow();

            // Integer expects int, passing long should fail validation
            Assert.Throws<InvalidOperationException>(() => dbRow.AppendValue(1L).EndRow());
        }
    }

    [Fact]
    public void TryAppendIntToBigInt()
    {
        Command.CommandText = "create table appender_type_test_bigint (value bigint);";

        Command.ExecuteNonQuery();

        using var appender = Connection.CreateAppender("", "appender_type_test_bigint");
        {
            var dbRow = appender.CreateRow();

            // BigInt expects long, passing int should fail validation
            Assert.Throws<InvalidOperationException>(() => dbRow.AppendValue(1).EndRow());
        }
    }

    [Fact]
    public void TryAppendIntToUnsignedTinyInt()
    {
        Command.CommandText = "create table appender_type_test_utinyint (value utinyint);";

        Command.ExecuteNonQuery();

        using var appender = Connection.CreateAppender("", "appender_type_test_utinyint");
        {
            var dbRow = appender.CreateRow();

            // UTINYINT expects byte, passing int should fail validation
            Assert.Throws<InvalidOperationException>(() => dbRow.AppendValue(1).EndRow());
        }
    }

    [Fact]
    public void TryAppendIntToUnsignedSmallInt()
    {
        Command.CommandText = "create table appender_type_test_usmallint (value usmallint);";

        Command.ExecuteNonQuery();

        using var appender = Connection.CreateAppender("", "appender_type_test_usmallint");
        {
            var dbRow = appender.CreateRow();

            // USMALLINT expects ushort, passing int should fail validation
            Assert.Throws<InvalidOperationException>(() => dbRow.AppendValue(1).EndRow());
        }
    }

    [Fact]
    public void TryAppendLongToUnsignedBigInt()
    {
        Command.CommandText = "create table appender_type_test_ubigint (value ubigint);";

        Command.ExecuteNonQuery();

        using var appender = Connection.CreateAppender("", "appender_type_test_ubigint");
        {
            var dbRow = appender.CreateRow();

            // UBIGINT expects ulong, passing long should fail validation
            Assert.Throws<InvalidOperationException>(() => dbRow.AppendValue(1L).EndRow());
        }
    }
}