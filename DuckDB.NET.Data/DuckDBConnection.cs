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
    /// The native connection, a <c>duckdb_v2_connection_handle</c> of DuckDB's C API V2 (<c>duckdb_v2.h</c>), for calling
    /// that API directly.
    /// </summary>
    /// <remarks>
    /// The handle belongs to this connection: it is valid only while the connection is open, and must not be destroyed.
    /// Do not use it while a command of this connection runs, or while a data reader of it is open: DuckDB allows one
    /// open result per connection.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The connection is not open.</exception>
    public IntPtr NativeConnection
    {
        get
        {
            // A closed connection still holds its reference, whose handle is destroyed: the state decides, not the reference.
            EnsureConnectionOpen();
            return V2Connection.DangerousGetHandle();
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
