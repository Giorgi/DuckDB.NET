using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Test.V2;

// DuckDBConnection.NativeConnection is the connection's duckdb_v2_connection_handle, for calling the C API V2 directly.
public unsafe class V2NativeConnectionTests
{
    [Fact]
    public void TheHandleIsTheConnectionsV2Connection()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:;threads=3");
        connection.Open();

        var handle = connection.NativeConnection;
        handle.Should().NotBe(IntPtr.Zero);
        connection.NativeConnection.Should().Be(handle, "the connection keeps one native connection while it is open");

        // It is the connection the commands run on, and a V2 call takes it (the config is borrowed, not destroyed).
        handle.Should().Be(connection.V2Connection.DangerousGetHandle());

        _duckdb_v2_error_info* error = null;
        _duckdb_v2_config* config;
        NativeMethodsV2.duckdb_v2_connection_get_config((_duckdb_v2_connection*)handle, &config, &error).ThrowIfFailed(ref error);
        ((IntPtr)config).Should().NotBe(IntPtr.Zero);
    }

    [Fact]
    public void TheHandleIsOnlyAvailableWhileTheConnectionIsOpen()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Invoking(c => c.NativeConnection).Should().Throw<InvalidOperationException>("the connection was never opened");

        connection.Open();
        _ = connection.NativeConnection;

        connection.Close();
        connection.Invoking(c => c.NativeConnection).Should().Throw<InvalidOperationException>("the connection is closed");
    }

    [Fact]
    public void TheArrowAppenderIsNotAvailableYet()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Invoking(c => c.CreateArrowAppender("t")).Should().Throw<InvalidOperationException>("the connection is closed");

        connection.Open();
        connection.ExecuteNonQuery("CREATE TABLE t (i INTEGER)");
        connection.Invoking(c => c.CreateArrowAppender("t")).Should().Throw<NotSupportedException>().WithMessage("*not been migrated*");
    }
}
