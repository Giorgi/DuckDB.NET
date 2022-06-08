using DuckDB.NET;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace DuckDB.NET.Test
{
    [TestClass]
    public class QueryTests
    {
        [TestMethod]
        public void Test()
        {
            var result = PlatformIndependentBindings.NativeMethods.DuckDBOpen(null, out var database);
            result.Should().Be(DuckDBState.DuckDBSuccess);

            result = PlatformIndependentBindings.NativeMethods.DuckDBConnect(database, out var connection);
            result.Should().Be(DuckDBState.DuckDBSuccess);

            using (database)
            using (connection)
            {
                var table = "CREATE TABLE test(a INTEGER, b BOOLEAN);";
                result = PlatformIndependentBindings.NativeMethods.DuckDBQuery(connection, table.ToUnmanagedString(), null);
                result.Should().Be(DuckDBState.DuckDBSuccess);


                var queryResult = new DuckDBResult();
                var insert = "INSERT INTO test VALUES (1, TRUE), (2, FALSE), (3, TRUE);";
                result = PlatformIndependentBindings.NativeMethods.DuckDBQuery(connection, insert.ToUnmanagedString(), queryResult);
                result.Should().Be(DuckDBState.DuckDBSuccess);

                var rowsChanged = PlatformIndependentBindings.NativeMethods.DuckDBRowsChanged(queryResult);
                rowsChanged.Should().Be(3);

                PlatformIndependentBindings.NativeMethods.DuckDBDestroyResult(queryResult);


                var query = "SELECT * FROM test;";
                result = PlatformIndependentBindings.NativeMethods.DuckDBQuery(connection, query.ToUnmanagedString(), queryResult);
                result.Should().Be(DuckDBState.DuckDBSuccess);

                var rowCount = PlatformIndependentBindings.NativeMethods.DuckDBRowCount(queryResult);
                rowCount.Should().Be(3);

                var columnCount = PlatformIndependentBindings.NativeMethods.DuckDBColumnCount(queryResult);
                columnCount.Should().Be(2);

                PlatformIndependentBindings.NativeMethods.DuckDBDestroyResult(queryResult);
            }
        }
    }
}

