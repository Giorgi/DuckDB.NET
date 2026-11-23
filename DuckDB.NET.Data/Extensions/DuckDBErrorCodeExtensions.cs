namespace DuckDB.NET.Data.Extensions;

internal static class DuckDBErrorCodeExtensions
{
    /// <summary>
    /// Throws when a V2 call failed. Reads the text of the error info that the call left in its err slot, destroys
    /// the info and sets the slot to zero. An interrupt becomes <see cref="OperationCanceledException"/>, as it does
    /// for V1 results.
    /// </summary>
    /// <param name="code">The code the call returned. V2 reports success only through it: the err slot may be left
    /// untouched.</param>
    /// <param name="errorInfo">The err slot passed to the call, holding zero beforehand.</param>
    /// <param name="fallbackMessage">The message when DuckDB reports no text.</param>
    public static void ThrowIfFailed(this DuckDBErrorCode code, ref IntPtr errorInfo, string? fallbackMessage = null)
    {
        if (code == DuckDBErrorCode.None)
        {
            return;
        }

        var message = ReadAndDestroy(ref errorInfo);

        if (code == DuckDBErrorCode.RuntimeInterrupt)
        {
            throw new OperationCanceledException(message);
        }

        if (string.IsNullOrEmpty(message))
        {
            message = fallbackMessage ?? $"DuckDB call failed with {code}";
        }

        throw new DuckDBException(message, code);
    }

    private static unsafe string ReadAndDestroy(ref IntPtr errorInfo)
    {
        if (errorInfo == IntPtr.Zero)
        {
            return string.Empty;
        }

        try
        {
            return NativeMethodsV2.ErrorInfo.DuckDBV2ErrorInfoGetText(errorInfo, out var text, out var length) == DuckDBErrorCode.None
                ? DuckDBV2Utf8.ToManaged(text, length)
                : string.Empty;
        }
        finally
        {
            NativeMethodsV2.ErrorInfo.DuckDBV2ErrorInfoDestroy(ref errorInfo);
        }
    }

    /// <summary>
    /// The <see cref="DuckDBErrorType"/> of a V2 error code. DuckDB derives each V2 code from the engine's exception
    /// type (GetErrorCodeFromExceptionType in src/main/capi/v2/capi_v2.cpp), and <see cref="DuckDBErrorType"/> is that
    /// exception type, so the mapping is its inverse. The codes V2 adds go to the nearest type.
    /// </summary>
    public static DuckDBErrorType ToErrorType(this DuckDBErrorCode code) => code switch
    {
        DuckDBErrorCode.InputInvalid => DuckDBErrorType.InvalidInput,
        DuckDBErrorCode.InputOutOfRange => DuckDBErrorType.OutOfRange,
        DuckDBErrorCode.InputObjectSize => DuckDBErrorType.ObjectSize,

        DuckDBErrorCode.IoGeneral => DuckDBErrorType.Io,
        DuckDBErrorCode.IoNetwork => DuckDBErrorType.Network,
        DuckDBErrorCode.IoHttp => DuckDBErrorType.Http,

        DuckDBErrorCode.ResourceOutOfMemory => DuckDBErrorType.OutOfMemory,
        DuckDBErrorCode.ResourceConnection => DuckDBErrorType.Connection,
        DuckDBErrorCode.ResourceDependency => DuckDBErrorType.Dependency,
        DuckDBErrorCode.ResourceMissingExtension => DuckDBErrorType.MissingExtension,
        DuckDBErrorCode.ResourceAutoload => DuckDBErrorType.Autoload,

        DuckDBErrorCode.TypeConversion => DuckDBErrorType.Conversion,
        DuckDBErrorCode.TypeUnknown => DuckDBErrorType.UnknownType,
        DuckDBErrorCode.TypeInvalid => DuckDBErrorType.InvalidType,
        DuckDBErrorCode.TypeMismatch => DuckDBErrorType.MismatchType,
        DuckDBErrorCode.TypeDecimal => DuckDBErrorType.Decimal,
        DuckDBErrorCode.TypeDivideByZero => DuckDBErrorType.DivideByZero,

        DuckDBErrorCode.QueryParser => DuckDBErrorType.Parser,
        DuckDBErrorCode.QuerySyntax => DuckDBErrorType.Syntax,
        DuckDBErrorCode.QueryBinder => DuckDBErrorType.Binder,
        DuckDBErrorCode.QueryPlanner => DuckDBErrorType.Planner,
        DuckDBErrorCode.QueryOptimizer => DuckDBErrorType.Optimizer,
        DuckDBErrorCode.QueryExpression => DuckDBErrorType.Expression,
        DuckDBErrorCode.QueryExecutor => DuckDBErrorType.Executor,
        DuckDBErrorCode.QueryScheduler => DuckDBErrorType.Scheduler,
        DuckDBErrorCode.QueryNotImplemented => DuckDBErrorType.NotImplemented,
        DuckDBErrorCode.QueryParameterNotResolved => DuckDBErrorType.ParameterNotResolved,
        DuckDBErrorCode.QueryParameterNotAllowed => DuckDBErrorType.ParameterNotAllowed,

        DuckDBErrorCode.DatabaseCatalog => DuckDBErrorType.Catalog,
        DuckDBErrorCode.DatabaseTransaction => DuckDBErrorType.Transaction,
        DuckDBErrorCode.DatabaseConstraint => DuckDBErrorType.Constraint,
        DuckDBErrorCode.DatabaseIndex => DuckDBErrorType.Index,
        DuckDBErrorCode.DatabaseSequence => DuckDBErrorType.Sequence,
        DuckDBErrorCode.DatabaseStatistics => DuckDBErrorType.Stat,
        DuckDBErrorCode.DatabaseSerialization => DuckDBErrorType.Serialization,

        DuckDBErrorCode.ConfigurationSettings => DuckDBErrorType.Settings,
        // INVALID_CONFIGURATION has no DuckDBErrorType of its own.
        DuckDBErrorCode.ConfigurationInvalid => DuckDBErrorType.Settings,
        DuckDBErrorCode.ConfigurationPermission => DuckDBErrorType.Permission,

        DuckDBErrorCode.RuntimeInternal => DuckDBErrorType.Internal,
        DuckDBErrorCode.RuntimeFatal => DuckDBErrorType.Fatal,
        DuckDBErrorCode.RuntimeInterrupt => DuckDBErrorType.Interrupt,
        DuckDBErrorCode.RuntimeNullPointer => DuckDBErrorType.NullPointer,

        // The codes V2 adds, which no engine exception type produces.
        DuckDBErrorCode.IoFileNotFound or DuckDBErrorCode.IoReadFailure or DuckDBErrorCode.IoEof => DuckDBErrorType.Io,
        DuckDBErrorCode.InputParameterInvalid or DuckDBErrorCode.InputMetadataDeferred => DuckDBErrorType.InvalidInput,

        // RESOURCE_IN_USE (a connection with an open result, an environment with live instances), API misuse, and
        // anything newer than this mapping. NativeErrorCode on DuckDBException carries the exact code.
        _ => DuckDBErrorType.Invalid,
    };
}
