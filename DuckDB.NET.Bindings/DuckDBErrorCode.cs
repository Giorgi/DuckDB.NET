namespace DuckDB.NET.Native;

/// <summary>
/// The error codes of the DuckDB C API V2 (<c>DUCKDB_V2_ERROR</c> in <c>duckdb_v2.h</c>). Every V2 function returns
/// one; <see cref="None"/> is the only success. The codes are grouped by their thousands: 1xxx IO, 2xxx input,
/// 3xxx resource, 4xxx type, 5xxx query, 6xxx database, 7xxx configuration and 8xxx runtime.
/// </summary>
public enum DuckDBErrorCode
{
    None = 0,

    /// <summary>The API was misused: an argument was null or invalid, or the error has no more specific code.</summary>
    Api = 1,

    IoFileNotFound = 1001,
    IoReadFailure = 1002,
    IoEof = 1003,
    IoGeneral = 1004,
    IoNetwork = 1005,
    IoHttp = 1006,

    InputInvalid = 2001,
    InputParameterInvalid = 2002,
    InputOutOfRange = 2003,
    InputObjectSize = 2004,
    InputMetadataDeferred = 2005,

    ResourceInUse = 3001,
    ResourceOutOfMemory = 3002,
    ResourceConnection = 3003,
    ResourceDependency = 3004,
    ResourceMissingExtension = 3005,
    ResourceAutoload = 3006,

    TypeConversion = 4001,
    TypeUnknown = 4002,
    TypeInvalid = 4003,
    TypeMismatch = 4004,
    TypeDecimal = 4005,
    TypeDivideByZero = 4006,

    QueryParser = 5001,
    QuerySyntax = 5002,
    QueryBinder = 5003,
    QueryPlanner = 5004,
    QueryOptimizer = 5005,
    QueryExpression = 5006,
    QueryExecutor = 5007,
    QueryScheduler = 5008,
    QueryNotImplemented = 5009,
    QueryParameterNotResolved = 5010,
    QueryParameterNotAllowed = 5011,

    DatabaseCatalog = 6001,
    DatabaseTransaction = 6002,
    DatabaseConstraint = 6003,
    DatabaseIndex = 6004,
    DatabaseSequence = 6005,
    DatabaseStatistics = 6006,
    DatabaseSerialization = 6007,

    ConfigurationSettings = 7001,
    ConfigurationInvalid = 7002,
    ConfigurationPermission = 7003,

    RuntimeInternal = 8001,
    RuntimeFatal = 8002,
    RuntimeInterrupt = 8003,
    RuntimeNullPointer = 8004,
}
