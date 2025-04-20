using DuckDB.NET.Native;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DuckDB.NET.Data.PreparedStatement;

internal sealed class PreparedStatement : IDisposable
{
    private readonly DuckDBPreparedStatement statement;

    internal bool UseStreamingMode { get; set; }

    internal PreparedStatement(DuckDBPreparedStatement statement)
    {
        this.statement = statement;
    }

    internal DuckDBResult Execute()
    {
        var status = UseStreamingMode
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

    internal void BindParameters(DuckDBParameterCollection parameterCollection)
    {
        var expectedParameters = NativeMethods.PreparedStatements.DuckDBParams(statement);
        if (parameterCollection.Count < expectedParameters)
        {
            throw new InvalidOperationException($"Invalid number of parameters. Expected {expectedParameters}, got {parameterCollection.Count}");
        }

        if (parameterCollection.OfType<DuckDBParameter>().Any(p => !string.IsNullOrEmpty(p.ParameterName)))
        {
            foreach (DuckDBParameter param in parameterCollection)
            {
                var state = NativeMethods.PreparedStatements.DuckDBBindParameterIndex(statement, out var index, param.ParameterName.ToUnmanagedString());
                if (state.IsSuccess())
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

    private void BindParameter(long index, DuckDBParameter parameter)
    {
        using var parameterLogicalType = NativeMethods.PreparedStatements.DuckDBParamLogicalType(statement, index);
        var duckDBType = NativeMethods.LogicalType.DuckDBGetTypeId(parameterLogicalType);

        using var duckDBValue = parameter.Value.ToDuckDBValue(parameterLogicalType, duckDBType);

        var result = NativeMethods.PreparedStatements.DuckDBBindValue(statement, index, duckDBValue);

        if (!result.IsSuccess())
        {
            var errorMessage = NativeMethods.PreparedStatements.DuckDBPrepareError(statement).ToManagedString(false);
            throw new InvalidOperationException($"Unable to bind parameter {index}: {errorMessage}");
        }
    }

    public void Dispose()
    {
        statement.Dispose();
    }
}