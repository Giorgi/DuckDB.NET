using DuckDB.NET.Native;
using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class QueryTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void QueryTest()
    {
        var result = NativeMethods.Startup.DuckDBOpen((string)null, out var database);
        result.Should().Be(DuckDBState.Success);

        result = NativeMethods.Startup.DuckDBConnect(database, out var connection);
        result.Should().Be(DuckDBState.Success);

        using (database)
        using (connection)
        {
            var table = "CREATE TABLE queryTest(a INTEGER, b BOOLEAN);";
            result = NativeMethods.Query.DuckDBQuery(connection, table.ToUnmanagedString(), out _);
            result.Should().Be(DuckDBState.Success);

            var insert = "INSERT INTO queryTest VALUES (1, TRUE), (2, FALSE), (3, TRUE);";
            result = NativeMethods.Query.DuckDBQuery(connection, insert.ToUnmanagedString(), out var queryResult);
            result.Should().Be(DuckDBState.Success);

            var rowsChanged = NativeMethods.Query.DuckDBRowsChanged(ref queryResult);
            rowsChanged.Should().Be(3);

            NativeMethods.Query.DuckDBDestroyResult(ref queryResult);


            var query = "SELECT * FROM queryTest;";
            result = NativeMethods.Query.DuckDBQuery(connection, query.ToUnmanagedString(), out queryResult);
            result.Should().Be(DuckDBState.Success);

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
            using var columnALogicalType = NativeMethods.Vectors.DuckDBVectorGetColumnType(columnA);
            var columnAType = NativeMethods.LogicalType.DuckDBGetTypeId(columnALogicalType);
            columnAType.Should().Be(DuckDBType.Integer);

            var columnB = NativeMethods.DataChunks.DuckDBDataChunkGetVector(chunk, 1);
            using var columnBLogicalType = NativeMethods.Vectors.DuckDBVectorGetColumnType(columnB);
            var columnBType = NativeMethods.LogicalType.DuckDBGetTypeId(columnBLogicalType);
            columnBType.Should().Be(DuckDBType.Boolean);

            NativeMethods.Query.DuckDBDestroyResult(ref queryResult);
        }
    }

    [Fact]
    public unsafe void ChunkTest()
    {
        var result = NativeMethods.Startup.DuckDBOpen((string)null, out var database);
        result.Should().Be(DuckDBState.Success);

        result = NativeMethods.Startup.DuckDBConnect(database, out var connection);
        result.Should().Be(DuckDBState.Success);

        using (database)
        using (connection)
        {
            var tableQuery = "CREATE TABLE integers (i INTEGER, j INTEGER);";
            result = NativeMethods.Query.DuckDBQuery(connection, tableQuery.ToUnmanagedString(), out _);
            result.Should().Be(DuckDBState.Success);

            var insertQuery = "INSERT INTO integers VALUES (3, 4), (5, 6);";
            result = NativeMethods.Query.DuckDBQuery(connection, insertQuery.ToUnmanagedString(), out _);
            result.Should().Be(DuckDBState.Success);

            var selectQuery = "SELECT * FROM integers;";
            result = NativeMethods.Query.DuckDBQuery(connection, selectQuery.ToUnmanagedString(), out var queryResult);
            result.Should().Be(DuckDBState.Success);

            using (var chunk = NativeMethods.Query.DuckDBFetchChunk(queryResult))
            {
                var rowCount = NativeMethods.DataChunks.DuckDBDataChunkGetSize(chunk);
                rowCount.Should().Be(2);

                var col1 = NativeMethods.DataChunks.DuckDBDataChunkGetVector(chunk, 0);
                var col1Data = NativeMethods.Vectors.DuckDBVectorGetData(col1);

                var col2 = NativeMethods.DataChunks.DuckDBDataChunkGetVector(chunk, 1);
                var col2Data = NativeMethods.Vectors.DuckDBVectorGetData(col2);

                var row0Col1Value = *(int*)col1Data;
                row0Col1Value.Should().Be(3);
                var row0Col2Value = *(int*)col2Data;
                row0Col2Value.Should().Be(4);
                var row1Col1Value = *((int*)col1Data + 1);
                row1Col1Value.Should().Be(5);
                var row2Col2Value = *((int*)col2Data + 1);
                row2Col2Value.Should().Be(6);
            }
        }
    }
}
