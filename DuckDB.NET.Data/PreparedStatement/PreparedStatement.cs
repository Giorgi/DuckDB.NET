using DuckDB.NET.Data.Connection;

namespace DuckDB.NET.Data.PreparedStatement;

internal class PreparedStatement : IDisposable
{
    protected readonly DuckDBPreparedStatement Statement;

    internal PreparedStatement(DuckDBPreparedStatement statement)
    {
        Statement = statement;
    }

    protected virtual bool RequiresClearBindings => false;

    protected virtual long ParameterCount => NativeMethods.PreparedStatements.DuckDBParams(Statement);

    public static IEnumerable<DuckDBResult> PrepareMultiple(DuckDBNativeConnection connection, string query, DuckDBParameterCollection parameters, bool useStreamingMode)
    {
        var statementCount = NativeMethods.ExtractStatements.DuckDBExtractStatements(connection, query, out var extractedStatements);

        using (extractedStatements)
        {
            if (statementCount <= 0)
            {
                var error = NativeMethods.ExtractStatements.DuckDBExtractStatementsError(extractedStatements);
                throw new DuckDBException(error);
            }

            for (int index = 0; index < statementCount; index++)
            {
                var status = NativeMethods.ExtractStatements.DuckDBPrepareExtractedStatement(connection, extractedStatements, index, out var statement);

                if (status.IsSuccess())
                {
                    using var preparedStatement = new PreparedStatement(statement);
                    yield return preparedStatement.Execute(parameters, useStreamingMode, connection);
                }
                else
                {
                    var errorMessage = NativeMethods.PreparedStatements.DuckDBPrepareError(statement);
                    statement.Dispose();

                    if (string.IsNullOrEmpty(errorMessage))
                    {
                        errorMessage = "DuckDBQuery failed";
                    }

                    throw new DuckDBException(errorMessage, UdfExceptionStore.Retrieve(connection));
                }
            }
        }
    }

    public static ReusablePreparedStatement? TryPrepareReusable(DuckDBNativeConnection connection, string query)
    {
        var statementCount = NativeMethods.ExtractStatements.DuckDBExtractStatements(connection, query, out var extractedStatements);

        using (extractedStatements)
        {
            if (statementCount <= 0)
            {
                var error = NativeMethods.ExtractStatements.DuckDBExtractStatementsError(extractedStatements);
                throw new DuckDBException(error);
            }

            // DuckDB can expand one logical command into dependent statements. Dynamic PIVOT,
            // IMPORT and some PRAGMAs require earlier statements to execute before later ones can
            // be prepared. Keep those commands on the existing per-execution path.
            if (statementCount != 1)
            {
                return null;
            }

            var status = NativeMethods.ExtractStatements.DuckDBPrepareExtractedStatement(connection, extractedStatements, 0, out var statement);
            if (status.IsSuccess())
            {
                return new ReusablePreparedStatement(statement);
            }

            var errorMessage = NativeMethods.PreparedStatements.DuckDBPrepareError(statement);
            statement.Dispose();

            if (string.IsNullOrEmpty(errorMessage))
            {
                errorMessage = "DuckDBQuery failed";
            }

            throw new DuckDBException(errorMessage, UdfExceptionStore.Retrieve(connection));
        }
    }

    internal DuckDBResult Execute(DuckDBParameterCollection parameterCollection, bool useStreamingMode, DuckDBNativeConnection connection)
    {
        if (RequiresClearBindings)
        {
            var clearState = NativeMethods.PreparedStatements.DuckDBClearBindings(Statement);
            if (!clearState.IsSuccess())
            {
                var errorMessage = NativeMethods.PreparedStatements.DuckDBPrepareError(Statement);
                throw new InvalidOperationException($"Unable to clear prepared statement bindings: {errorMessage}");
            }
        }

        BindParameters(parameterCollection);

        var status = useStreamingMode
            ? NativeMethods.PreparedStatements.DuckDBExecutePreparedStreaming(Statement, out var queryResult)
            : NativeMethods.PreparedStatements.DuckDBExecutePrepared(Statement, out queryResult);

        if (!status.IsSuccess())
        {
            var errorMessage = NativeMethods.Query.DuckDBResultError(ref queryResult);
            var errorType = NativeMethods.Query.DuckDBResultErrorType(ref queryResult);
            queryResult.Close();

            if (string.IsNullOrEmpty(errorMessage))
            {
                errorMessage = "DuckDB execution failed";
            }

            if (errorType == DuckDBErrorType.Interrupt)
            {
                throw new OperationCanceledException();
            }

            var innerException = UdfExceptionStore.Retrieve(connection);
            throw innerException != null
                ? new DuckDBException(errorMessage, innerException)
                : new DuckDBException(errorMessage, errorType);
        }

        return queryResult;
    }

    private void BindParameters(DuckDBParameterCollection parameterCollection)
    {
        var expectedParameters = ParameterCount;
        if (parameterCollection.Count < expectedParameters)
        {
            throw new InvalidOperationException($"Invalid number of parameters. Expected {expectedParameters}, got {parameterCollection.Count}");
        }

        // Index-based iteration over the typed collection avoids the per-execution allocations of
        // OfType<>().Any(...) and of the boxed List enumerator that `foreach (DuckDBParameter ...)`
        // over the non-generic collection would produce. BindParameters runs on every execution.
        var count = parameterCollection.Count;

        var hasNamedParameters = false;
        for (var i = 0; i < count; i++)
        {
            if (!string.IsNullOrEmpty(parameterCollection[i].ParameterName))
            {
                hasNamedParameters = true;
                break;
            }
        }

        if (hasNamedParameters)
        {
            for (var i = 0; i < count; i++)
            {
                var param = parameterCollection[i];
                if (TryGetParameterIndex(param.ParameterName, out var index))
                {
                    BindParameter(index, param);
                }
            }
        }
        else
        {
            for (var i = 0; i < expectedParameters; ++i)
            {
                var param = parameterCollection[i];
                BindParameter(i + 1, param);
            }
        }
    }

    protected virtual bool TryGetParameterIndex(string parameterName, out long index)
    {
        if (string.IsNullOrEmpty(parameterName))
        {
            index = 0;
            return false;
        }

        var state = NativeMethods.PreparedStatements.DuckDBBindParameterIndex(Statement, out var nativeIndex, parameterName);
        index = state.IsSuccess() ? nativeIndex : 0;

        return index > 0;
    }

    protected virtual void BindParameter(long index, DuckDBParameter parameter)
    {
        using var parameterLogicalType = NativeMethods.PreparedStatements.DuckDBParamLogicalType(Statement, index);
        BindParameter(index, parameter, parameterLogicalType);
    }

    protected void BindParameter(long index, DuckDBParameter parameter, DuckDBLogicalType parameterLogicalType)
    {
        var duckDBType = NativeMethods.LogicalType.DuckDBGetTypeId(parameterLogicalType);

        using var duckDBValue = parameter.Value.ToDuckDBValue(parameterLogicalType, duckDBType, parameter.DbType);

        var result = NativeMethods.PreparedStatements.DuckDBBindValue(Statement, index, duckDBValue);

        if (!result.IsSuccess())
        {
            var errorMessage = NativeMethods.PreparedStatements.DuckDBPrepareError(Statement);
            throw new InvalidOperationException($"Unable to bind parameter {index}: {errorMessage}");
        }
    }

    public virtual void Dispose()
    {
        Statement.Dispose();
    }
}

internal sealed class ReusablePreparedStatement : PreparedStatement
{
    private readonly long cachedParameterCount;
    private readonly DuckDBLogicalType[] cachedParameterTypes;
    private readonly Dictionary<string, long> cachedParameterIndices = new(StringComparer.Ordinal);

    public ReusablePreparedStatement(DuckDBPreparedStatement statement)
        : base(statement)
    {
        cachedParameterCount = NativeMethods.PreparedStatements.DuckDBParams(statement);
        cachedParameterTypes = new DuckDBLogicalType[cachedParameterCount];

        try
        {
            for (var index = 0; index < cachedParameterTypes.Length; index++)
            {
                cachedParameterTypes[index] = NativeMethods.PreparedStatements.DuckDBParamLogicalType(statement, index + 1);
            }
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    protected override bool RequiresClearBindings => true;

    protected override long ParameterCount => cachedParameterCount;

    protected override bool TryGetParameterIndex(string parameterName, out long index)
    {
        if (cachedParameterIndices.TryGetValue(parameterName, out index))
        {
            return index > 0;
        }

        var found = base.TryGetParameterIndex(parameterName, out index);
        cachedParameterIndices.Add(parameterName, index);

        return found;
    }

    protected override void BindParameter(long index, DuckDBParameter parameter)
    {
        BindParameter(index, parameter, cachedParameterTypes[index - 1]);
    }

    public override void Dispose()
    {
        foreach (var parameterType in cachedParameterTypes)
        {
            parameterType?.Dispose();
        }

        base.Dispose();
    }
}
