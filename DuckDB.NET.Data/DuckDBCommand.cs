using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data.Internal;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data;

public class DuckDBCommand : DbCommand
{
    private DuckDBConnection? connection;
    private readonly DuckDBParameterCollection parameters = new();
    private QueryProgressObservable? progressObservable;

    protected override DbTransaction? DbTransaction { get; set; }
    protected override DbParameterCollection DbParameterCollection => parameters;

    public new virtual DuckDBParameterCollection Parameters => parameters;

    public override int CommandTimeout { get; set; }
    public override CommandType CommandType { get; set; }
    public override bool DesignTimeVisible { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }

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
            commandText = value ?? string.Empty;
        }
    }

    protected override DbConnection? DbConnection
    {
        get => connection;
        set => connection = (DuckDBConnection?)value;
    }

    public DuckDBCommand() { }

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

        // Check progress if callback is set
        if (ProgressCallback != null)
        {
            progressObservable = new QueryProgressObservable(connection);
            progressObservable.Subscribe(new ProgressObserver(ProgressCallback));
            progressObservable.Start();
        }

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

    public Action<int>? ProgressCallback { get; set; }

    private class ProgressObserver : IObserver<int>
    {
        private readonly Action<int> progressCallback;

        public ProgressObserver(Action<int> progressCallback)
        {
            this.progressCallback = progressCallback;
        }

        public void OnCompleted() { }

        public void OnError(Exception error) { }

        public void OnNext(int value)
        {
            progressCallback(value);
        }
    }
}