using DuckDB.NET.Data.Connection;

namespace DuckDB.NET.Data.PreparedStatement;

internal sealed class PreparedStatement : IDisposable
{
    private readonly DuckDBPreparedStatement statement;

    private PreparedStatement(DuckDBPreparedStatement statement)
    {
        this.statement = statement;
    }

    public static IEnumerable<StatementResult> PrepareMultiple(DuckDBNativeConnection connection, string query, DuckDBParameterCollection parameters, bool useStreamingMode)
    {
        var statementCount = NativeMethods.ExtractStatements.DuckDBExtractStatements(connection, query, out var extractedStatements);

        using (extractedStatements)
        {
            if (statementCount == 0)
            {
                var error = NativeMethods.ExtractStatements.DuckDBExtractStatementsError(extractedStatements);
                throw new DuckDBException(error);
            }

            for (ulong index = 0; index < statementCount; index++)
            {
                var status = NativeMethods.ExtractStatements.DuckDBPrepareExtractedStatement(connection, extractedStatements, (long)index, out var statement);

                if (status.IsSuccess())
                {
                    using var preparedStatement = new PreparedStatement(statement);
                    var statementType = NativeMethods.PreparedStatements.DuckDBPreparedStatementType(statement);
                    yield return new StatementResult(preparedStatement.Execute(parameters, useStreamingMode, connection), statementType);
                }
                else
                {
                    throw PrepareFailed(statement, connection);
                }
            }
        }
    }

    /// <summary>
    /// Prepares a command that holds exactly one statement, to be executed many times. A command of several statements
    /// returns null: a later statement can depend on an earlier one having run, so each is prepared just before it runs.
    /// </summary>
    public static PreparedStatementEntry? PrepareSingle(DuckDBNativeConnection connection, string query)
    {
        var statementCount = NativeMethods.ExtractStatements.DuckDBExtractStatements(connection, query, out var extractedStatements);

        using (extractedStatements)
        {
            if (statementCount == 0)
            {
                var error = NativeMethods.ExtractStatements.DuckDBExtractStatementsError(extractedStatements);
                throw new DuckDBException(string.IsNullOrEmpty(error) ? "The command text holds no statement to prepare" : error);
            }

            if (statementCount != 1)
            {
                return null;
            }

            var status = NativeMethods.ExtractStatements.DuckDBPrepareExtractedStatement(connection, extractedStatements, 0, out var statement);

            if (!status.IsSuccess())
            {
                using (statement)
                {
                    throw PrepareFailed(statement, connection);
                }
            }

            try
            {
                return new PreparedStatementEntry(statement);
            }
            catch
            {
                statement.Dispose();
                throw;
            }
        }
    }

    private static DuckDBException PrepareFailed(DuckDBPreparedStatement statement, DuckDBNativeConnection connection)
    {
        var errorMessage = NativeMethods.PreparedStatements.DuckDBPrepareError(statement);

        if (string.IsNullOrEmpty(errorMessage))
        {
            errorMessage = "DuckDBQuery failed";
        }

        return new DuckDBException(errorMessage, UdfExceptionStore.Retrieve(connection));
    }

    private DuckDBResult Execute(DuckDBParameterCollection parameterCollection, bool useStreamingMode, DuckDBNativeConnection connection)
    {
        BindParameters(statement, parameterCollection, null);

        return Execute(statement, useStreamingMode, connection);
    }

    internal static DuckDBResult Execute(DuckDBPreparedStatement statement, bool useStreamingMode, DuckDBNativeConnection connection)
    {
        var status = useStreamingMode
            ? NativeMethods.PreparedStatements.DuckDBExecutePreparedStreaming(statement, out var queryResult)
            : NativeMethods.PreparedStatements.DuckDBExecutePrepared(statement, out queryResult);

        if (!status.IsSuccess())
        {
            try
            {
                queryResult.ThrowOnError(connection, "DuckDB execution failed");
            }
            finally
            {
                queryResult.Close();
            }
        }

        return queryResult;
    }

    /// <summary>
    /// Binds the parameters of the collection to the statement. With an entry, the parameter count, the type of each
    /// parameter and the index of each name come from what the entry read when it was prepared; without one, they
    /// are asked of DuckDB.
    /// </summary>
    internal static void BindParameters(DuckDBPreparedStatement preparedStatement, DuckDBParameterCollection parameterCollection, PreparedStatementEntry? entry)
    {
        var expectedParameters = entry?.ParameterCount ?? NativeMethods.PreparedStatements.DuckDBParams(preparedStatement);
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
                if (TryGetParameterIndex(preparedStatement, param.ParameterName, entry, out var index))
                {
                    BindParameter(preparedStatement, index, param, entry);
                }
            }
        }
        else
        {
            for (var i = 0; i < expectedParameters; ++i)
            {
                var param = parameterCollection[i];
                BindParameter(preparedStatement, i + 1, param, entry);
            }
        }
    }

    private static bool TryGetParameterIndex(DuckDBPreparedStatement preparedStatement, string name, PreparedStatementEntry? entry, out long index)
    {
        if (entry is not null)
        {
            return entry.TryGetParameterIndex(name, out index);
        }

        var found = NativeMethods.PreparedStatements.DuckDBBindParameterIndex(preparedStatement, out var nativeIndex, name).IsSuccess();
        index = nativeIndex;
        return found;
    }

    private static void BindParameter(DuckDBPreparedStatement preparedStatement, long index, DuckDBParameter parameter, PreparedStatementEntry? entry)
    {
        if (entry is not null)
        {
            var (logicalType, duckDBType) = entry.GetParameterType(index);
            BindValue(preparedStatement, index, parameter, logicalType, duckDBType);
            return;
        }

        using var parameterLogicalType = NativeMethods.PreparedStatements.DuckDBParamLogicalType(preparedStatement, index);
        BindValue(preparedStatement, index, parameter, parameterLogicalType, NativeMethods.LogicalType.DuckDBGetTypeId(parameterLogicalType));
    }

    private static void BindValue(DuckDBPreparedStatement preparedStatement, long index, DuckDBParameter parameter, DuckDBLogicalType logicalType, DuckDBType duckDBType)
    {
        using var duckDBValue = parameter.Value.ToDuckDBValue(logicalType, duckDBType, parameter.DbType);

        var result = NativeMethods.PreparedStatements.DuckDBBindValue(preparedStatement, index, duckDBValue);

        if (!result.IsSuccess())
        {
            var errorMessage = NativeMethods.PreparedStatements.DuckDBPrepareError(preparedStatement);
            throw new InvalidOperationException($"Unable to bind parameter {index}: {errorMessage}");
        }
    }

    public void Dispose()
    {
        statement.Dispose();
    }
}