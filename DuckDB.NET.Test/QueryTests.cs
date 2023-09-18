using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

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
            var table = "CREATE TABLE queryTest(a INTEGER, b BOOLEAN);";
            result = NativeMethods.Query.DuckDBQuery(connection, table.ToUnmanagedString(), out _);
            result.Should().Be(DuckDBState.DuckDBSuccess);

            var insert = "INSERT INTO queryTest VALUES (1, TRUE), (2, FALSE), (3, TRUE);";
            result = NativeMethods.Query.DuckDBQuery(connection, insert.ToUnmanagedString(), out var queryResult);
            result.Should().Be(DuckDBState.DuckDBSuccess);

            var rowsChanged = NativeMethods.Query.DuckDBRowsChanged(ref queryResult);
            rowsChanged.Should().Be(3);

            NativeMethods.Query.DuckDBDestroyResult(ref queryResult);


            var query = "SELECT * FROM queryTest;";
            result = NativeMethods.Query.DuckDBQuery(connection, query.ToUnmanagedString(), out queryResult);
            result.Should().Be(DuckDBState.DuckDBSuccess);

            var rowCount = NativeMethods.Query.DuckDBRowCount(ref queryResult);
            rowCount.Should().Be(3);

            var columnCount = NativeMethods.Query.DuckDBColumnCount(ref queryResult);
            columnCount.Should().Be(2);

            // Using Data Chunks API
            var chunkCount = NativeMethods.Types.DuckDBResultChunkCount(queryResult);
            chunkCount.Should().Be(1);

            using var chunk = NativeMethods.Types.DuckDBResultGetChunk(queryResult, 0);
            var columnCountInChunk = NativeMethods.DataChunks.DuckDBDataChunkGetColumnCount(chunk);
            columnCountInChunk.Should().Be(2);
            var chunkRowCount = NativeMethods.DataChunks.DuckDBDataChunkGetSize(chunk);
            chunkRowCount.Should().Be(3);

            var columnA = NativeMethods.DataChunks.DuckDBDataChunkGetVector(chunk, 0);
            using var columnALogicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(columnA);
            var columnAType = NativeMethods.LogicalType.DuckDBGetTypeId(columnALogicalType);
            columnAType.Should().Be(DuckDBType.DuckdbTypeInteger);

            var columnB = NativeMethods.DataChunks.DuckDBDataChunkGetVector(chunk, 1);
            using var columnBLogicalType = NativeMethods.DataChunks.DuckDBVectorGetColumnType(columnB);
            var columnBType = NativeMethods.LogicalType.DuckDBGetTypeId(columnBLogicalType);
            columnBType.Should().Be(DuckDBType.DuckdbTypeBoolean);

            NativeMethods.Query.DuckDBDestroyResult(ref queryResult);
        }
    }
}
