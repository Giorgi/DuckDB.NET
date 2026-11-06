using DuckDB.NET.Data.Connection;
using DuckDB.NET.Data.PreparedStatement;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace DuckDB.NET.Data;

public partial class DuckDBConnection : DbConnection
{
    private readonly ConnectionManager connectionManager = ConnectionManager.Default;
    private ConnectionState connectionState = ConnectionState.Closed;
    private DuckDBConnectionString? parsedConnection;
    private ConnectionReference? connectionReference;
    private bool inMemoryDuplication = false;
    private readonly object preparedStatementsLock = new();
    private Dictionary<string, PreparedStatementEntry>? preparedStatements;

    private static readonly StateChangeEventArgs FromClosedToOpenEventArgs = new(ConnectionState.Closed, ConnectionState.Open);
    private static readonly StateChangeEventArgs FromOpenToClosedEventArgs = new(ConnectionState.Open, ConnectionState.Closed);

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

    [AllowNull]
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

    /// <summary>
    /// Returns the native connection object that can be used to call DuckDB C API functions.
    /// </summary>
    public DuckDBNativeConnection NativeConnection => connectionReference?.NativeConnection
                                                      ?? throw new InvalidOperationException("The DuckDBConnection must be open to access the native connection.");

    public override string ServerVersion => NativeMethods.Startup.DuckDBLibraryVersion();

    public override ConnectionState State => connectionState;

    public override void ChangeDatabase(string databaseName)
    {
        throw new NotSupportedException();
    }

    public override void Close()
    {
        // DbConnection.Close: "An application can call Close or CloseAsync more than one time. No exception is generated."
        if (connectionState == ConnectionState.Closed)
        {
            return;
        }

        // Before the native connection goes back: the database must not outlive ConnectionManager's last reference.
        DisposePreparedStatements();

        if (connectionReference is not null) //Should always be the case
        {
            connectionManager.ReturnConnectionReference(connectionReference);
        }

        connectionState = ConnectionState.Closed;
        OnStateChange(FromOpenToClosedEventArgs);
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

        OpenCount++;
        connectionState = ConnectionState.Open;
        OnStateChange(FromClosedToOpenEventArgs);
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

    public DuckDBAppender CreateAppender(string table) => CreateAppender(null, null, table);

    public DuckDBAppender CreateAppender(string? schema, string table) => CreateAppender(null, schema, table);

    public DuckDBAppender CreateAppender(string? catalog, string? schema, string table)
    {
        EnsureConnectionOpen();

        var appenderState = NativeMethods.Appender.DuckDBAppenderCreateExt(NativeConnection, catalog, schema, table, out var nativeAppender);

        if (!appenderState.IsSuccess())
        {
            try
            {
                NativeMethods.Appender.DuckDBAppenderErrorData(nativeAppender).ThrowOnError();
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

    /// <summary>
    /// Creates a type-safe appender using an AppenderMap for property-to-column mappings.
    /// </summary>
    /// <typeparam name="T">The type to append</typeparam>
    /// <typeparam name="TMap">The AppenderMap type defining the mappings</typeparam>
    /// <param name="table">The table name</param>
    /// <returns>A type-safe mapped appender</returns>
    public DuckDBMappedAppender<T, TMap> CreateAppender<T, TMap>(string table) 
        where TMap : Mapping.DuckDBAppenderMap<T>, new()
    {
        return CreateAppender<T, TMap>(null, null, table);
    }

    /// <summary>
    /// Creates a type-safe appender using an AppenderMap for property-to-column mappings.
    /// </summary>
    /// <typeparam name="T">The type to append</typeparam>
    /// <typeparam name="TMap">The AppenderMap type defining the mappings</typeparam>
    /// <param name="schema">The schema name</param>
    /// <param name="table">The table name</param>
    /// <returns>A type-safe mapped appender</returns>
    public DuckDBMappedAppender<T, TMap> CreateAppender<T, TMap>(string? schema, string table)
        where TMap : Mapping.DuckDBAppenderMap<T>, new()
    {
        return CreateAppender<T, TMap>(null, schema, table);
    }

    /// <summary>
    /// Creates a type-safe appender using an AppenderMap for property-to-column mappings.
    /// </summary>
    /// <typeparam name="T">The type to append</typeparam>
    /// <typeparam name="TMap">The AppenderMap type defining the mappings</typeparam>
    /// <param name="catalog">The catalog name</param>
    /// <param name="schema">The schema name</param>
    /// <param name="table">The table name</param>
    /// <returns>A type-safe mapped appender</returns>
    public DuckDBMappedAppender<T, TMap> CreateAppender<T, TMap>(string? catalog, string? schema, string table)
        where TMap : Mapping.DuckDBAppenderMap<T>, new()
    {
        var appender = CreateAppender(catalog, schema, table);

        try
        {
            return new DuckDBMappedAppender<T, TMap>(appender);
        }
        catch
        {
            // The mapping did not pass validation, so nobody else gets to dispose the appender.
            appender.Dispose();
            throw;
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            // this check is to ensure exact same behavior as previous version
            // where Close() was calling Dispose(true) instead of the other way around.
            if (connectionState == ConnectionState.Open)
            {
                Close();
            }
        }

        base.Dispose(disposing);
    }

    /// <summary>
    /// How many times the connection was opened. A prepared command records it, so it can tell that the connection
    /// was closed in between (which destroyed its statement) and prepare the statement again.
    /// </summary>
    internal int OpenCount { get; private set; }

    /// <summary>
    /// The statement prepared for the command text on this connection, prepared now if it is not kept yet. Null for
    /// a command of several statements, which is prepared on each execution instead. Entries are kept until the
    /// connection closes; commands with the same text share one.
    /// </summary>
    internal PreparedStatementEntry? GetOrPrepareStatement(string commandText)
    {
        lock (preparedStatementsLock)
        {
            preparedStatements ??= new Dictionary<string, PreparedStatementEntry>(StringComparer.Ordinal);

            if (preparedStatements.TryGetValue(commandText, out var entry) && !entry.IsDisposed)
            {
                return entry;
            }

            entry = PreparedStatement.PreparedStatement.PrepareSingle(NativeConnection, commandText);
            if (entry is not null)
            {
                preparedStatements[commandText] = entry;
            }

            return entry;
        }
    }

    private void DisposePreparedStatements()
    {
        lock (preparedStatementsLock)
        {
            if (preparedStatements is null)
            {
                return;
            }

            foreach (var entry in preparedStatements.Values)
            {
                entry.Dispose();
            }

            preparedStatements = null;
        }
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

    public override DataTable GetSchema() =>
        GetSchema(DbMetaDataCollectionNames.MetaDataCollections);

    public override DataTable GetSchema(string collectionName) =>
        GetSchema(collectionName, null);

    public override DataTable GetSchema(string collectionName, string?[]? restrictionValues) =>
        DuckDBSchema.GetSchema(this, collectionName, restrictionValues);

    public DuckDBQueryProgress GetQueryProgress()
    {
        EnsureConnectionOpen();
        return NativeMethods.Startup.DuckDBQueryProgress(NativeConnection);
    }
}
