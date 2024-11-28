using System;
using System.Collections.Generic;
using System.Linq;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal;

internal sealed class PreparedStatement : IDisposable
{
    private readonly DuckDBPreparedStatement statement;

    private PreparedStatement(DuckDBPreparedStatement statement)
    {
        this.statement = statement;
    }

    public static IEnumerable<DuckDBResult> PrepareMultiple(DuckDBNativeConnection connection, string query, DuckDBParameterCollection parameters, bool useStreamingMode)
    {
        using var unmanagedQuery = query.ToUnmanagedString();

        var statementCount = NativeMethods.ExtractStatements.DuckDBExtractStatements(connection, unmanagedQuery, out var extractedStatements);

        using (extractedStatements)
        {
            if (statementCount <= 0)
            {
                var error = NativeMethods.ExtractStatements.DuckDBExtractStatementsError(extractedStatements);
                throw new DuckDBException(error.ToManagedString(false));
            }

            for (int index = 0; index < statementCount; index++)
            {
                var status = NativeMethods.ExtractStatements.DuckDBPrepareExtractedStatement(connection, extractedStatements, index, out var statement);

                if (status.IsSuccess())
                {
                    using var preparedStatement = new PreparedStatement(statement);
                    using var result = preparedStatement.Execute(parameters, useStreamingMode);
                    yield return result;
                }
                else
                {
                    var errorMessage = NativeMethods.PreparedStatements.DuckDBPrepareError(statement).ToManagedString(false);

                    throw new DuckDBException(string.IsNullOrEmpty(errorMessage) ? "DuckDBQuery failed" : errorMessage);
                }
            }
        }
    }

    private DuckDBResult Execute(DuckDBParameterCollection parameterCollection, bool useStreamingMode)
    {
        BindParameters(statement, parameterCollection);

        var status = useStreamingMode
            ? NativeMethods.PreparedStatements.DuckDBExecutePreparedStreaming(statement, out var queryResult)
            : NativeMethods.PreparedStatements.DuckDBExecutePrepared(statement, out queryResult);

        if (!status.IsSuccess())
        {
            var errorMessage = NativeMethods.Query.DuckDBResultError(ref queryResult).ToManagedString(false);
            var errorType = NativeMethods.Query.DuckDBResultErrorType(ref queryResult);
            queryResult.Dispose();

            if (string.IsNullOrEmpty(errorMessage))
            {
                errorMessage = "DuckDB execution failed";
            }

            if (errorType == DuckDBErrorType.Interrupt)
            {
                throw new OperationCanceledException();
            }

            throw new DuckDBException(errorMessage, errorType);
        }

        return queryResult;
    }

    private static void BindParameters(DuckDBPreparedStatement preparedStatement, DuckDBParameterCollection parameterCollection)
    {
        var expectedParameters = NativeMethods.PreparedStatements.DuckDBParams(preparedStatement);
        if (parameterCollection.Count < expectedParameters)
        {
            throw new InvalidOperationException($"Invalid number of parameters. Expected {expectedParameters}, got {parameterCollection.Count}");
        }

        if (parameterCollection.OfType<DuckDBParameter>().Any(p => !string.IsNullOrEmpty(p.ParameterName)))
        {
            foreach (DuckDBParameter param in parameterCollection)
            {
                var state = NativeMethods.PreparedStatements.DuckDBBindParameterIndex(preparedStatement, out var index, param.ParameterName.ToUnmanagedString());
                if (state.IsSuccess())
                {
                    BindParameter(preparedStatement, index, param);
                }
            }
        }
        else
        {
            for (var i = 0; i < expectedParameters; ++i)
            {
                var param = parameterCollection[i];
                BindParameter(preparedStatement, i + 1, param);
            }
        }
    }

    private static void BindParameter(DuckDBPreparedStatement preparedStatement, long index, DuckDBParameter parameter)
    {
        using var parameterLogicalType = NativeMethods.PreparedStatements.DuckDBParamLogicalType(preparedStatement, index);
        using var duckDBValue = parameter.Value.ToDuckDBValue(parameterLogicalType);

        var result = NativeMethods.PreparedStatements.DuckDBBindValue(preparedStatement, index, duckDBValue);

        if (!result.IsSuccess())
        {
            var errorMessage = NativeMethods.PreparedStatements.DuckDBPrepareError(preparedStatement).ToManagedString(false);
            throw new InvalidOperationException($"Unable to bind parameter {index}: {errorMessage}");
        }
    }

    public void Dispose()
    {
        statement.Dispose();
    }
}