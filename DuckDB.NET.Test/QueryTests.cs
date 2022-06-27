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
            var result = NativeMethods.DuckDBOpen(null, out var database);
            result.Should().Be(DuckDBState.DuckDBSuccess);

            result = NativeMethods.DuckDBConnect(database, out var connection);
            result.Should().Be(DuckDBState.DuckDBSuccess);

            using (database)
            using (connection)
            {
                var table = "CREATE TABLE test(a INTEGER, b BOOLEAN);";
                result = NativeMethods.DuckDBQuery(connection, table.ToUnmanagedString(), null);
                result.Should().Be(DuckDBState.DuckDBSuccess);


                var queryResult = new DuckDBResult();
                var insert = "INSERT INTO test VALUES (1, TRUE), (2, FALSE), (3, TRUE);";
                result = NativeMethods.DuckDBQuery(connection, insert.ToUnmanagedString(), queryResult);
                result.Should().Be(DuckDBState.DuckDBSuccess);

                var rowsChanged = NativeMethods.DuckDBRowsChanged(queryResult);
                rowsChanged.Should().Be(3);

                NativeMethods.DuckDBDestroyResult(queryResult);


                var query = "SELECT * FROM test;";
                result = NativeMethods.DuckDBQuery(connection, query.ToUnmanagedString(), queryResult);
                result.Should().Be(DuckDBState.DuckDBSuccess);

                var rowCount = NativeMethods.DuckDBRowCount(queryResult);
                rowCount.Should().Be(3);

                var columnCount = NativeMethods.DuckDBColumnCount(queryResult);
                columnCount.Should().Be(2);

                NativeMethods.DuckDBDestroyResult(queryResult);
            }
        }
    }
}

