using System;
using System.Collections.Generic;

namespace DuckDB.NET.Data;

internal static class DuckDBStatementExecutor
{
    private static readonly Dictionary<Type, Func<DuckDBPreparedStatement, long, object, DuckDBState>> Binders =
        new Dictionary<Type, Func<DuckDBPreparedStatement, long, object, DuckDBState>>
        {
            {typeof(bool), BindBoolean},
            {typeof(sbyte), BindInt8},
            {typeof(short), BindInt16},
            {typeof(int), BindInt32},
            {typeof(long), BindInt64},
            {typeof(float), BindFloat},
            {typeof(double), BindDouble},
            {typeof(string), BindString},
        };

    public static DuckDBQueryResult Execute(DuckDBNativeConnection connection, string query, DuckDBDbParameterCollection parameterCollection)
    {
        using var unmanagedQuery = query.ToUnmanagedString();
        var queryResult = new DuckDBResult();
        DuckDBPreparedStatement preparedStatement = null;
        try
        {
            var result = NativeMethods.PreparedStatements.DuckDBPrepare(connection, unmanagedQuery, out preparedStatement);
            if (!result.IsSuccess())
            {
                var errorMessage = NativeMethods.PreparedStatements.DuckDBPrepareError(preparedStatement).ToManagedString(false);
                throw new DuckDBException(string.IsNullOrEmpty(errorMessage) ? "DuckDBQuery failed" : errorMessage, result);
            }

            BindParameters(preparedStatement, parameterCollection);
            
            result = NativeMethods.PreparedStatements.DuckDBExecutePrepared(preparedStatement, queryResult);
            if (!result.IsSuccess())
            {
                var errorMessage = NativeMethods.Query.DuckDBResultError(queryResult).ToManagedString(false);
                throw new DuckDBException(string.IsNullOrEmpty(errorMessage) ? "DuckDBQuery failed" : errorMessage, result);
            }

            var tmp = queryResult;
            queryResult = null;
            return new DuckDBQueryResult(tmp);
        }
        finally
        {
            preparedStatement?.Dispose();
            if (queryResult != null)
                NativeMethods.Query.DuckDBDestroyResult(queryResult);
        }
    }

    private static void BindParameters(DuckDBPreparedStatement preparedStatement, DuckDBDbParameterCollection parameterCollection)
    {
        if (parameterCollection == null || parameterCollection.Count == 0) return;

        var expectedParameters = NativeMethods.PreparedStatements.DuckDBParams(preparedStatement);
        if (expectedParameters != parameterCollection.Count)
            throw new InvalidOperationException($"Invalid number of parameters. Expected {expectedParameters}, got {parameterCollection.Count}");
        
        for (var i = 0; i < parameterCollection.Count; ++i)
        {
            var param = (DuckDBParameter)parameterCollection[i];
            BindParameter(preparedStatement, i + 1, param);
        }
    }

    private static void BindParameter(DuckDBPreparedStatement preparedStatement, long index, DuckDBParameter parameter)
    {
        if (parameter.Value == null)
        {
            NativeMethods.PreparedStatements.DuckDBBindNull(preparedStatement, index);
            return;
        }

        var paramType = parameter.Value.GetType();
        if (!Binders.TryGetValue(paramType, out var binder))
            throw new InvalidOperationException($"Unable to bind value of type {paramType.FullName}.");
        var result = binder(preparedStatement, index, parameter.Value);
        if (!result.IsSuccess())
        {
            var errorMessage = NativeMethods.PreparedStatements.DuckDBPrepareError(preparedStatement).ToManagedString(false);
            throw new InvalidOperationException($"Unable to bind parameter {index}: {errorMessage}");
        }
    }

    private static DuckDBState BindBoolean(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindBoolean(preparedStatement, index, (bool) value);
    
    private static DuckDBState BindInt8(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindInt8(preparedStatement, index, (sbyte) value);
    
    private static DuckDBState BindInt16(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindInt16(preparedStatement, index, (short) value);
    
    private static DuckDBState BindInt32(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindInt32(preparedStatement, index, (int) value);

    private static DuckDBState BindInt64(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindInt64(preparedStatement, index, (long) value);
    
    private static DuckDBState BindFloat(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindFloat(preparedStatement, index, (float) value);
    
    private static DuckDBState BindDouble(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindDouble(preparedStatement, index, (double) value);
    
    private static DuckDBState BindString(DuckDBPreparedStatement preparedStatement, long index, object value)
    {
        using var unmanagedString = ((string) value).ToUnmanagedString();
        return NativeMethods.PreparedStatements.DuckDBBindVarchar(preparedStatement, index, unmanagedString);
    }
}