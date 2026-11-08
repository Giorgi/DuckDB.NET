namespace DuckDB.NET.Test;

/// <summary>
/// Calls the bindings whose parameter or return types were declared with the wrong width (see
/// <see cref="NativeSignatureTests"/>), with values that only come out right when the declaration matches duckdb.h.
/// </summary>
public class NativeBindingWidthTests
{
    [Fact]
    public void ExtractStatementsReturnsTheStatementCount()
    {
        using var native = new NativeConnection();

        var count = NativeMethods.ExtractStatements.DuckDBExtractStatements(native.Connection, "SELECT 1; SELECT 2; SELECT 3", out var statements);
        using (statements)
        {
            count.Should().Be(3UL);
        }

        var failed = NativeMethods.ExtractStatements.DuckDBExtractStatements(native.Connection, "SELECT FROM WHERE", out var invalid);
        using (invalid)
        {
            failed.Should().Be(0UL);
            NativeMethods.ExtractStatements.DuckDBExtractStatementsError(invalid).Should().Contain("syntax error");
        }
    }

    [Fact]
    public void AppendVarcharWithLengthAppendsThatManyBytes()
    {
        using var native = new NativeConnection();
        native.Execute("CREATE TABLE strings(v VARCHAR)");

        native.Append("strings", appender =>
            NativeMethods.Appender.DuckDBAppendVarchar(appender, "hello world", 5UL).Should().Be(DuckDBState.Success));

        native.QueryBoolean("SELECT v = 'hello' FROM strings").Should().BeTrue();
    }

    [Fact]
    public unsafe void AppendBlobAppendsThatManyBytes()
    {
        using var native = new NativeConnection();
        native.Execute("CREATE TABLE blobs(b BLOB)");

        byte[] bytes = [1, 2, 3, 4, 5];
        native.Append("blobs", appender =>
        {
            fixed (byte* data = bytes)
            {
                NativeMethods.Appender.DuckDBAppendBlob(appender, data, 3UL).Should().Be(DuckDBState.Success);
            }
        });

        native.QueryBoolean("SELECT b = '\\x01\\x02\\x03'::BLOB FROM blobs").Should().BeTrue();
    }

    [Fact]
    public void DataChunkSetSizeSetsTheSize()
    {
        using var integer = NativeMethods.LogicalType.DuckDBCreateLogicalType(DuckDBType.Integer);
        using var chunk = NativeMethods.DataChunks.DuckDBCreateDataChunk([integer.DangerousGetHandle()], 1);

        NativeMethods.DataChunks.DuckDBDataChunkSetSize(chunk, 7);

        NativeMethods.DataChunks.DuckDBDataChunkGetSize(chunk).Should().Be(7UL);
    }

    [Fact]
    public void GetConfigFlagSucceedsForEveryIndexBelowTheCount()
    {
        var count = NativeMethods.Configuration.DuckDBConfigCount();
        count.Should().BeGreaterThan((nuint)100);

        NativeMethods.Configuration.DuckDBGetConfigFlag(count - 1, out var name, out _).Should().Be(DuckDBState.Success);
        name.Should().NotBeNullOrEmpty();

        NativeMethods.Configuration.DuckDBGetConfigFlag(count, out _, out _).Should().Be(DuckDBState.Error);
    }

    // AppendDefaultToChunk is exercised through DuckDBAppenderRow.AppendDefault in DuckDBManagedAppenderTests.

    private sealed class NativeConnection : IDisposable
    {
        private readonly DuckDBDatabase database;

        public NativeConnection()
        {
            NativeMethods.Startup.DuckDBOpen((string)null, out database).Should().Be(DuckDBState.Success);
            NativeMethods.Startup.DuckDBConnect(database, out var connection).Should().Be(DuckDBState.Success);
            Connection = connection;
        }

        public DuckDBNativeConnection Connection { get; }

        public void Execute(string sql)
        {
            NativeMethods.Query.DuckDBQuery(Connection, sql, out var result).Should().Be(DuckDBState.Success);
            NativeMethods.Query.DuckDBDestroyResult(ref result);
        }

        public bool QueryBoolean(string sql)
        {
            NativeMethods.Query.DuckDBQuery(Connection, sql, out var result).Should().Be(DuckDBState.Success);
            try
            {
                NativeMethods.Query.DuckDBRowCount(ref result).Should().Be(1);
                return NativeMethods.Types.DuckDBValueBoolean(ref result, 0, 0);
            }
            finally
            {
                NativeMethods.Query.DuckDBDestroyResult(ref result);
            }
        }

        public void Append(string table, Action<DuckDB.NET.Native.DuckDBAppender> appendValue)
        {
            NativeMethods.Appender.DuckDBAppenderCreate(Connection, null, table, out var appender).Should().Be(DuckDBState.Success);
            using (appender)
            {
                appendValue(appender);
                NativeMethods.Appender.DuckDBAppenderEndRow(appender).Should().Be(DuckDBState.Success);
            }
        }

        public void Dispose()
        {
            Connection.Dispose();
            database.Dispose();
        }
    }
}
