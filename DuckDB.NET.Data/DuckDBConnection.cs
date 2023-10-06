using DuckDB.NET.Data.Internal;
using System;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using DuckDB.NET.Data.ConnectionString;

namespace DuckDB.NET.Data;

public class DuckDBConnection : DbConnection
{
    private readonly ConnectionManager connectionManager = ConnectionManager.Default;
    private ConnectionState connectionState = ConnectionState.Closed;
    private DuckDBConnectionString? connectionString;
    private ConnectionReference? connectionReference;
    private bool inMemoryDuplication;

    #region Protected Properties

    protected override DbProviderFactory DbProviderFactory => DuckDBClientFactory.Instance;

    #endregion

    internal DuckDBTransaction? Transaction { get; set; }

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
    public DuckDBConnection()
    { }

    public DuckDBConnection(string connectionString)

    {
        ConnectionString = connectionString;
    }

#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.

#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
    public override string ConnectionString { get; set; }
#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).

    public override string Database { get; }

    public override string DataSource { get; }

    internal DuckDBNativeConnection? NativeConnection => connectionReference?.NativeConnection;

#pragma warning disable CS8603 // Possible null reference return.
    public override string ServerVersion => NativeMethods.Startup.DuckDBLibraryVersion().ToManagedString(false);
#pragma warning restore CS8603 // Possible null reference return.

    public override ConnectionState State => connectionState;

    public override void ChangeDatabase(string databaseName)
    {
        throw new NotSupportedException();
    }

    public override void Close()
    {
        if (connectionState == ConnectionState.Closed)
        {
            throw new InvalidOperationException("Connection is already closed.");
        }

        Dispose(true);
    }

    public override void Open()
    {
        if (connectionState == ConnectionState.Open)
        {
            throw new InvalidOperationException("Connection is already open.");
        }

        if (inMemoryDuplication)
        {
            connectionReference = connectionManager.DuplicateConnectionReference(connectionReference);
        }
        else
        {
            connectionString = DuckDBConnectionStringParser.Parse(ConnectionString);

            connectionReference = connectionManager.GetConnectionReference(connectionString);
        }

        connectionState = ConnectionState.Open;
    }

    protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        return BeginTransaction(isolationLevel);
    }

    public new DuckDBTransaction BeginTransaction()
    {
        return BeginTransaction(IsolationLevel.Unspecified);
    }

    private new DuckDBTransaction BeginTransaction(IsolationLevel isolationLevel)
    {
        EnsureConnectionOpen();
        if (Transaction != null)
        {
            throw new InvalidOperationException("Already in a transaction.");
        }

        return Transaction = new DuckDBTransaction(this, isolationLevel);
    }

    protected override DbCommand CreateDbCommand()
    {
        return CreateCommand();
    }

    public new virtual DuckDbCommand CreateCommand()
    {
        return new DuckDbCommand
        {
            Connection = this,
            Transaction = Transaction
        };
    }

    public DuckDBAppender CreateAppender(string table) => CreateAppender(null, table);

    public DuckDBAppender CreateAppender(string? schema, string table)
    {
        EnsureConnectionOpen();
        if (NativeMethods.Appender.DuckDBAppenderCreate(NativeConnection!, schema, table, out var nativeAppender) == DuckDBState.Error)
        {
            try
            {
                DuckDBAppender.ThrowLastError(nativeAppender);
            }
            finally
            {
                nativeAppender.Close();
            }
        }

        return new DuckDBAppender(nativeAppender);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            if (connectionState == ConnectionState.Open)
            {
                connectionManager.ReturnConnectionReference(connectionReference);
                connectionState = ConnectionState.Closed;
            }
        }

        base.Dispose(disposing);
    }

    private void EnsureConnectionOpen([CallerMemberName] string operation = "")
    {
        if (State != ConnectionState.Open)
        {
            throw new InvalidOperationException($"{operation} requires an open connection");
        }
    }

    public DuckDBConnection Duplicate()
    {
        if (State != ConnectionState.Open)
        {
            throw new InvalidOperationException("Duplication requires an open connection");
        }

        if (!(connectionString?.InMemory ?? false))
        {
            throw new NotSupportedException();
        }

        var duplicatedConnection = new DuckDBConnection(ConnectionString)
        {
            connectionString = connectionString,
            inMemoryDuplication = true,
            connectionReference = connectionReference,
        };

        return duplicatedConnection;
    }
}