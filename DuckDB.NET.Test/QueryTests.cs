using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test
{
    public class QueryTests
    {
        [Fact]
        public void QueryTest()
        {
            var result = NativeMethods.Startup.DuckDBOpen(null, out var database);
            result.Should().Be(DuckDBState.DuckDBSuccess);

            result = NativeMethods.Startup.DuckDBConnect(database, out var connection);
            result.Should().Be(DuckDBState.DuckDBSuccess);

            using (database)
            using (connection)
            {
                var table = "CREATE TABLE test(a INTEGER, b BOOLEAN);";
                result = NativeMethods.Query.DuckDBQuery(connection, table.ToUnmanagedString(), null);
                result.Should().Be(DuckDBState.DuckDBSuccess);


                var queryResult = new DuckDBResult();
                var insert = "INSERT INTO test VALUES (1, TRUE), (2, FALSE), (3, TRUE);";
                result = NativeMethods.Query.DuckDBQuery(connection, insert.ToUnmanagedString(), queryResult);
                result.Should().Be(DuckDBState.DuckDBSuccess);

                var rowsChanged = NativeMethods.Query.DuckDBRowsChanged(queryResult);
                rowsChanged.Should().Be(3);

                NativeMethods.Query.DuckDBDestroyResult(queryResult);


                var query = "SELECT * FROM test;";
                result = NativeMethods.Query.DuckDBQuery(connection, query.ToUnmanagedString(), queryResult);
                result.Should().Be(DuckDBState.DuckDBSuccess);

                var rowCount = NativeMethods.Query.DuckDBRowCount(queryResult);
                rowCount.Should().Be(3);

                var columnCount = NativeMethods.Query.DuckDBColumnCount(queryResult);
                columnCount.Should().Be(2);

                NativeMethods.Query.DuckDBDestroyResult(queryResult);
            }
        }
    }
}

