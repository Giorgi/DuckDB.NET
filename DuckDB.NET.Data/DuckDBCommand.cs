using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data;

public class DuckDbCommand : DbCommand
{
    private DuckDBConnection? connection;
    private readonly DuckDBParameterCollection parameters = new();

#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
    protected override DbTransaction DbTransaction { get; set; }
#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
    protected override DbParameterCollection DbParameterCollection => parameters;

    public new virtual DuckDBParameterCollection Parameters => parameters;

    public override int CommandTimeout { get; set; }
    public override CommandType CommandType { get; set; }
    public override bool DesignTimeVisible { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }

#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
    public override string CommandText { get; set; }

    protected override DbConnection DbConnection
    {
#pragma warning disable CS8603 // Possible null reference return.
        get => connection;
#pragma warning restore CS8603 // Possible null reference return.
        set => connection = (DuckDBConnection)value;
    }
#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).

    public DuckDbCommand() : this(string.Empty, null)
    {
    }

    public DuckDbCommand(string commandText) : this(commandText, null)
    {
        CommandText = commandText;
    }

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
    public DuckDbCommand(string commandText, DuckDBConnection? connection)
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
    {
        CommandText = commandText;
        Connection = connection;
    }

    public override void Cancel()
    {

    }

    public override int ExecuteNonQuery()
    {
        EnsureConnectionOpen();

        var (preparedStatements, results) = PreparedStatement.PrepareMultiple(connection!.NativeConnection!, CommandText, parameters);

        var count = 0;

        for (var index = 0; index < results.Count; index++)
        {
            var result = results[index];
            count += (int)NativeMethods.Query.DuckDBRowsChanged(ref result);

            result.Dispose();
        }

        foreach (var statement in preparedStatements)
        {
            statement.Dispose();
        }

        return count;
    }

    public override object ExecuteScalar()
    {
        EnsureConnectionOpen();

        using var reader = ExecuteReader();

#pragma warning disable CS8603 // Possible null reference return.
        return reader.Read() ? reader.GetValue(0) : null;
#pragma warning restore CS8603 // Possible null reference return.
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

        var (preparedStatements, results) = PreparedStatement.PrepareMultiple(connection!.NativeConnection!, CommandText, parameters);

        var reader = new DuckDBDataReader(this, results, behavior);

        foreach (var statement in preparedStatements)
        {
            statement.Dispose();
        }

        return reader;
    }

    public override void Prepare() { }

    protected override DbParameter CreateDbParameter() => new DuckDBParameter();

    internal void CloseConnection()
    {
        Connection?.Close();
    }

    private void EnsureConnectionOpen([CallerMemberName] string operation = "")
    {
        if (connection?.State != ConnectionState.Open)
        {
            throw new InvalidOperationException($"{operation} requires an open connection");
        }
    }
}