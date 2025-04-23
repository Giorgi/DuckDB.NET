using DuckDB.NET.Native;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data;

public class DuckDBCommand : DbCommand
{
    private DuckDBConnection? connection;
    private readonly DuckDBParameterCollection parameters = new();
    private bool prepared;
    private readonly List<PreparedStatement.PreparedStatement> preparedStatements = new();

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

    internal DuckDBDataReader? DataReader { get; set; }

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
            if (DataReader != null)
                throw new InvalidOperationException("channot change CommandText while a reader is open");

            if (commandText == value)
                return;

            DisposePreparedStatements();
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
            connection.NativeConnection.Interrupt();
        }
    }

    public override int ExecuteNonQuery()
    {
        EnsureConnectionOpen();

        var count = 0;

        foreach (var statement in GetStatements())
        {
            var current = statement.Execute();
            count += (int)NativeMethods.Query.DuckDBRowsChanged(ref current);
            current.Dispose();
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

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DataReader?.Dispose();
        }

        DisposePreparedStatements();

        base.Dispose(disposing);
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        if (DataReader != null)
            throw new InvalidOperationException("cannot create a new reader while one is open");

        EnsureConnectionOpen();

        var closeConnection = behavior.HasFlag(CommandBehavior.CloseConnection);

        return new DuckDBDataReader(this, GetStatements(), closeConnection);
    }

    public override void Prepare() { }

    protected override DbParameter CreateDbParameter() => new DuckDBParameter();

    internal void CloseConnection() => Connection!.Close();

    private void DisposePreparedStatements()
    {
        foreach (var statement in preparedStatements)
        {
            statement.Dispose();
        }

        preparedStatements.Clear();
        prepared = false;
    }

    private void EnsureConnectionOpen([CallerMemberName] string operation = "")
    {
        if (Connection is null || Connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException($"{operation} requires an open connection");
        }
    }

    private IEnumerable<PreparedStatement.PreparedStatement> GetStatements()
    {
        foreach (var statement in prepared
                     ? preparedStatements
                     : PrepareAndEnumerateStatements())
        {
            statement.BindParameters(Parameters);
            statement.UseStreamingMode = UseStreamingMode;
            yield return statement;
        }
    }

    private IEnumerable<PreparedStatement.PreparedStatement> PrepareAndEnumerateStatements()
    {
        DisposePreparedStatements();

        using var unmanagedQuery = CommandText.ToUnmanagedString();

        var statementCount = NativeMethods.ExtractStatements.DuckDBExtractStatements(connection!.NativeConnection, unmanagedQuery, out var extractedStatements);

        using (extractedStatements)
        {
            if (statementCount <= 0)
            {
                var error = NativeMethods.ExtractStatements.DuckDBExtractStatementsError(extractedStatements);
                throw new DuckDBException(error.ToManagedString(false));
            }

            for (int index = 0; index < statementCount; index++)
            {
                var status = NativeMethods.ExtractStatements.DuckDBPrepareExtractedStatement(connection!.NativeConnection, extractedStatements, index, out var unmanagedStatement);

                if (status.IsSuccess())
                {
                    var statement = new PreparedStatement.PreparedStatement(unmanagedStatement);
                    preparedStatements.Add(statement);
                    yield return statement;
                }
                else
                {
                    var errorMessage = NativeMethods.PreparedStatements.DuckDBPrepareError(unmanagedStatement).ToManagedString(false);

                    throw new DuckDBException(string.IsNullOrEmpty(errorMessage) ? "DuckDBQuery failed" : errorMessage);
                }
            }
        }

        prepared = true;
    }
}