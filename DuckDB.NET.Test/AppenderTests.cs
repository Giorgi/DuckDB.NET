using Xunit;
using FluentAssertions;

namespace DuckDB.NET.Test
{
    public class DuckDBAppenderTests
    {
        [Fact]
        public void AppenderTests()
        {
            var result = NativeMethods.Startup.DuckDBOpen(null, out var database);
            result.Should().Be(DuckDBState.DuckDBSuccess);

            result = NativeMethods.Startup.DuckDBConnect(database, out var connection);
            result.Should().Be(DuckDBState.DuckDBSuccess);

            using (database)
            using (connection)
            {
                var table = "CREATE TABLE appenderTest(a BOOLEAN, b TINYINT, c SMALLINT, d INTEGER, e BIGINT, f UTINYINT, g USMALLINT, h UINTEGER, i UBIGINT, j REAL, k DOUBLE, l VARCHAR);";
                result = NativeMethods.Query.DuckDBQuery(connection, table.ToUnmanagedString(), out var queryResult);
                result.Should().Be(DuckDBState.DuckDBSuccess);

                result = NativeMethods.Appender.DuckDBAppenderCreate(connection, null, "appenderTest", out var appender);
                result.Should().Be(DuckDBState.DuckDBSuccess);

                var rows = 10;
                using (appender)
                {
                    for (var i = 0; i < rows; i++)
                    {
                        NativeMethods.Appender.DuckDBAppendBool(appender, i % 2 == 0);
                        NativeMethods.Appender.DuckDBAppendInt8(appender, (sbyte)i);
                        NativeMethods.Appender.DuckDBAppendInt16(appender, (short)i);
                        NativeMethods.Appender.DuckDBAppendInt32(appender, (int)i);
                        NativeMethods.Appender.DuckDBAppendInt64(appender, (long)i);
                        NativeMethods.Appender.DuckDBAppendUInt8(appender, (byte)i);
                        NativeMethods.Appender.DuckDBAppendUInt16(appender, (ushort)i);
                        NativeMethods.Appender.DuckDBAppendUInt32(appender, (uint)i);
                        NativeMethods.Appender.DuckDBAppendUInt64(appender, (ulong)i);
                        NativeMethods.Appender.DuckDBAppendFloat(appender, (float)i);
                        NativeMethods.Appender.DuckDBAppendDouble(appender, (double)i);
                        NativeMethods.Appender.DuckDBAppendVarchar(appender, i.ToString().ToUnmanagedString());
                        NativeMethods.Appender.DuckDBAppenderEndRow(appender);
                    }
                }

                var query = "SELECT * FROM appenderTest";
                result = NativeMethods.Query.DuckDBQuery(connection, query.ToUnmanagedString(), out queryResult);
                result.Should().Be(DuckDBState.DuckDBSuccess);

                var rowCount = NativeMethods.Query.DuckDBRowCount(ref queryResult);
                rowCount.Should().Be(rows);

                for (var i = 0; i < rows; i++)
                {
                    NativeMethods.Types.DuckDBValueBoolean(ref queryResult, 0, i).Should().Be(i % 2 == 0);
                    NativeMethods.Types.DuckDBValueInt8(ref queryResult, 1, i).Should().Be((sbyte)i);
                    NativeMethods.Types.DuckDBValueInt16(ref queryResult, 2, i).Should().Be((short)i);
                    NativeMethods.Types.DuckDBValueInt32(ref queryResult, 3, i).Should().Be((int)i);
                    NativeMethods.Types.DuckDBValueInt64(ref queryResult, 4, i).Should().Be((long)i);
                    NativeMethods.Types.DuckDBValueFloat(ref queryResult, 9, i).Should().Be((float)i);
                    NativeMethods.Types.DuckDBValueDouble(ref queryResult, 10, i).Should().Be((double)i);
                }

                NativeMethods.Query.DuckDBDestroyResult(ref queryResult);
            }
        }
    }
}

