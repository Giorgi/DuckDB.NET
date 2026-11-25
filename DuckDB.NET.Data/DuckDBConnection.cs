using DuckDB.NET.Data.Connection;
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
            return string.IsNullOrEmpty(ConnectionString) switch
            {
                false => ParsedConnection.DataSource,
                _ => throw new InvalidOperationException("Connection string must be specified.")
            };
        }
    }

    public override string DataSource
    {
        get
        {
            return string.IsNullOrEmpty(ConnectionString) switch
            {
                false => ParsedConnection!.DataSource,
                _ => throw new InvalidOperationException("Connection string must be specified.")
            };
        }
    }

    /// <summary>
    /// Returns the native connection object that can be used to call DuckDB C API functions.
    /// </summary>
    /// <remarks>
    /// Not available on this branch: the connection is opened through DuckDB's C API V2, whose handles cannot be used
    /// with the V1 functions in <see cref="NativeMethods"/>. What replaces it is decided at the end of the migration
    /// (docs/V2-MIGRATION.md).
    /// </remarks>
    public DuckDBNativeConnection NativeConnection
    {
        get
        {
            _ = V2Connection;
            throw new NotSupportedException("The V1 native connection is not available: this build of DuckDB.NET opens connections " +
                                            "through DuckDB's C API V2, and this operation has not been migrated yet (docs/V2-MIGRATION.md).");
        }
    }

    internal DuckDBV2Connection V2Connection => connectionReference?.NativeConnection
                                                ?? throw new InvalidOperationException("The DuckDBConnection must be open to access the native connection.");

    public override string ServerVersion => DuckDBV2Startup.LibraryVersion();

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

        connectionReference = connectionManager.GetConnectionReference(ParsedConnection);

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

    private void EnsureConnectionOpen([CallerMemberName] string operation = "")
    {
        if (State != ConnectionState.Open)
        {
            throw new InvalidOperationException($"{operation} requires an open connection");
        }
    }

    /// <summary>
    /// No longer supported: always throws <see cref="NotSupportedException"/>.
    /// </summary>
    /// <remarks>
    /// To open several connections to one in-memory database, give the database a name: every connection with the
    /// data source <c>:memory:&lt;name&gt;</c> reaches the same database.
    /// </remarks>
    [Obsolete("Duplicate is no longer supported and always throws NotSupportedException. To share an in-memory database, " +
              "open every connection with the data source ':memory:<name>'.")]
    public DuckDBConnection Duplicate()
    {
        throw new NotSupportedException("Duplicate is no longer supported. To share an in-memory database, open every connection " +
                                        "with the data source ':memory:<name>'.");
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
        return DuckDBV2Startup.QueryProgress(V2Connection);
    }

    // Interrupts the query running on the connection; safe from any thread. Throws when the connection is closed, as
    // the V1 NativeConnection did.
    internal void Interrupt() => DuckDBV2Startup.Interrupt(V2Connection);
}
