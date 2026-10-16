namespace DuckDB.NET.Data;

public class DuckDBClientFactory : DbProviderFactory
{
    public const string ProviderInvariantName = "DuckDB.NET.Data";

    #region Static Properties

    public static readonly DuckDBClientFactory Instance = new();

    #endregion

    #region Properties

    public override bool CanCreateDataSourceEnumerator => false;

    #endregion

    #region Constructors

    private DuckDBClientFactory() { }

    #endregion

    #region Methods

    // DuckDB.NET has no command builder or data adapter. CreateCommandBuilder and CreateDataAdapter are not
    // overridden, so they return null and CanCreateCommandBuilder and CanCreateDataAdapter report false.

    public override DbCommand CreateCommand() => new DuckDBCommand();

    public override DbConnection CreateConnection() => new DuckDBConnection();

    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new DuckDBConnectionStringBuilder();

    public override DbParameter CreateParameter() => new DuckDBParameter();

    #endregion
}