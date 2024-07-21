using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using DuckDB.NET.Data.Internal;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data;

public class DuckDBCommand : DbCommand
{
    private DuckDBConnection? connection;
    private readonly DuckDBParameterCollection parameters = new();

    protected override DbTransaction? DbTransaction { get; set; }
    protected override DbParameterCollection DbParameterCollection => parameters;

    public new virtual DuckDBParameterCollection Parameters => parameters;

    public override int CommandTimeout { get; set; }
    public override CommandType CommandType { get; set; }
    public override bool DesignTimeVisible { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }

    /// <summary>
    /// A flag to determine whether to use streaming mode or not when executing a query. Defaults to false.
    /// In streaming mode DuckDB will use less RAM but query execution might be slower. Applies only to queries that return a result-set.
    /// </summary>
    /// <remarks>
    /// Streaming mode uses `duckdb_execute_prepared_streaming` and `duckdb_stream_fetch_chunk`, non-streaming (materialized) mode uses `duckdb_execute_prepared` and `duckdb_result_get_chunk`.
    /// </remarks>
    public bool UseStreamingMode { get; set; } = false;

    private string commandText = string.Empty;

#if NET6_0_OR_GREATER
    [AllowNull]
#endif
    [DefaultValue("")]
    public override string CommandText
    {
        get => commandText;
        set
        {
            // TODO: We shouldn't be able to change the CommandText when the command is in execution (requires CommandState implementation)
            commandText = value ?? string.Empty;
        }
    }

    protected override DbConnection? DbConnection
    {
        get => connection;
        set => connection = (DuckDBConnection?)value;
    }

    public DuckDBCommand()
    { }

    public DuckDBCommand(string commandText)
    {
        CommandText = commandText;
    }

    public DuckDBCommand(string commandText, DuckDBConnection connection)
        : this(commandText)
    {
        Connection = connection;
    }

    public override void Cancel()
    {
        if (connection != null)
        {
            NativeMethods.Startup.DuckDBInterrupt(connection.NativeConnection.DangerousGetHandle());
        }
    }

    public override int ExecuteNonQuery()
    {
        EnsureConnectionOpen();

        var results = PreparedStatement.PrepareMultiple(connection!.NativeConnection, CommandText, parameters, UseStreamingMode);

        var count = 0;

        foreach (var result in results)
        {
            var current = result;
            count += (int)NativeMethods.Query.DuckDBRowsChanged(ref current);
        }

        return count;
    }

    public override object? ExecuteScalar()
    {
        EnsureConnectionOpen();

        using var reader = ExecuteReader();
        return reader.Read() ? reader.GetValue(0) : null;
    }

    public new DuckDBDataReader ExecuteReader()
    {
        return (DuckDBDataReader)base.ExecuteReader();
    }

    public new DuckDBDataReader ExecuteReader(CommandBehavior behavior)
    {
        return (DuckDBDataReader)base.ExecuteReader(behavior);
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        EnsureConnectionOpen();

        var results = PreparedStatement.PrepareMultiple(connection!.NativeConnection, CommandText, parameters, UseStreamingMode);

        var reader = new DuckDBDataReader(this, results, behavior);

        return reader;
    }

    public override void Prepare() { }

    protected override DbParameter CreateDbParameter() => new DuckDBParameter();

    internal void CloseConnection() => Connection!.Close();

    private void EnsureConnectionOpen([CallerMemberName] string operation = "")
    {
        if (Connection is null || Connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException($"{operation} requires an open connection");
        }
    }
}