using DuckDB.NET.Data.ConnectionString;
using DuckDB.NET.Data.Internal;
using System;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data;

public class DuckDBConnection : DbConnection
{
    private readonly ConnectionManager connectionManager = ConnectionManager.Default;
    private ConnectionState connectionState = ConnectionState.Closed;
    private DuckDBConnectionString? parsedConnection;
    private ConnectionReference? connectionReference;
    private bool inMemoryDuplication = false;

    #region Protected Properties

    protected override DbProviderFactory? DbProviderFactory => DuckDBClientFactory.Instance;

    #endregion

    internal DuckDBTransaction? Transaction { get; set; }

    internal DuckDBConnectionString ParsedConnection => parsedConnection ??= DuckDBConnectionStringBuilder.Parse(ConnectionString);

    public DuckDBConnection()
    {
        ConnectionString = string.Empty;
    }

    public DuckDBConnection(string connectionString)
    {
        ConnectionString = connectionString;
    }

#if NET6_0_OR_GREATER
    [AllowNull]
#endif
    [DefaultValue("")]
    public override string ConnectionString { get; set; }

    public override string Database
    {
        get
        {
            if (!string.IsNullOrEmpty(ConnectionString))
            {
                return ParsedConnection.DataSource;
            }

            throw new InvalidOperationException("Connection string must be specified.");
        }
    }

    public override string DataSource
    {
        get
        {
            if (!string.IsNullOrEmpty(ConnectionString))
            {
                return ParsedConnection!.DataSource;
            }

            throw new InvalidOperationException("Connection string must be specified.");
        }
    }

    internal DuckDBNativeConnection NativeConnection => connectionReference?.NativeConnection
                                                        ?? throw new InvalidOperationException("The DuckDBConnection must be open to access the native connection.");

    public override string ServerVersion => NativeMethods.Startup.DuckDBLibraryVersion().ToManagedString(false);

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

        //In case of inMemoryDuplication, we can safely take the hypothesis that connectionReference is already assigned
        connectionReference = inMemoryDuplication ? connectionManager.DuplicateConnectionReference(connectionReference!)
                                                  : connectionManager.GetConnectionReference(ParsedConnection);

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

    public new virtual DuckDBCommand CreateCommand()
    {
        return new DuckDBCommand
        {
            Connection = this,
            Transaction = Transaction
        };
    }

    public DuckDBAppender CreateAppender(string table) => CreateAppender(null, table);

    public DuckDBAppender CreateAppender(string? schema, string table)
    {
        EnsureConnectionOpen();
        using var unmanagedSchema = schema.ToUnmanagedString();
        using var unmanagedTable = table.ToUnmanagedString();

        var appenderState = NativeMethods.Appender.DuckDBAppenderCreate(NativeConnection, unmanagedSchema, unmanagedTable, out var nativeAppender);

        if (!appenderState.IsSuccess())
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

        return new DuckDBAppender(nativeAppender, GetTableName());

        string GetTableName()
        {
            return string.IsNullOrEmpty(schema) ? table : $"{schema}.{table}";
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            if (connectionState == ConnectionState.Open)
            {
                if (connectionReference is not null) //Should always be the case
                {
                    connectionManager.ReturnConnectionReference(connectionReference);
                }
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
        EnsureConnectionOpen();

        // We're sure that the connectionString is not null because we previously checked the connection was open
        if (!ParsedConnection!.InMemory)
        {
            throw new NotSupportedException("Duplication of the connection is only supported for in-memory connections.");
        }

        var duplicatedConnection = new DuckDBConnection(ConnectionString)
        {
            parsedConnection = ParsedConnection,
            inMemoryDuplication = true,
            connectionReference = connectionReference,
        };

        return duplicatedConnection;
    }
}