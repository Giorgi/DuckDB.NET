using System;
using System.Collections.Generic;
using System.Data;

namespace DuckDB.NET.Data;

internal sealed class PreparedStatement : IDisposable
{
    private static readonly Dictionary<DbType, Func<DuckDBPreparedStatement, long, object, DuckDBState>> Binders = new()
    {
        { DbType.Boolean, BindBoolean },
        { DbType.SByte, BindInt8 },
        { DbType.Int16, BindInt16 },
        { DbType.Int32, BindInt32 },
        { DbType.Int64, BindInt64 },
        { DbType.Single, BindFloat },
        { DbType.Double, BindDouble },
        { DbType.String, BindString },
    };

    private readonly DuckDBPreparedStatement statement;

    private PreparedStatement(DuckDBPreparedStatement statement)
    {
        this.statement = statement;
    }

    public static PreparedStatement Prepare(DuckDBNativeConnection connection, string query)
    {
        using var unmanagedQuery = query.ToUnmanagedString();

        var status = NativeMethods.PreparedStatements.DuckDBPrepare(connection, unmanagedQuery, out var preparedStatement);
        if (!status.IsSuccess())
        {
            var errorMessage = NativeMethods.PreparedStatements.DuckDBPrepareError(preparedStatement).ToManagedString(false);
            preparedStatement.Dispose();
            throw new DuckDBException(string.IsNullOrEmpty(errorMessage) ? "DuckDBQuery failed" : errorMessage, status);
        }

        var result = new PreparedStatement(preparedStatement);
        return result;
    }

    public DuckDBResult Execute(DuckDBParameterCollection parameterCollection)
    {
        var queryResult = new DuckDBResult();
        BindParameters(statement, parameterCollection);

        var status = NativeMethods.PreparedStatements.DuckDBExecutePrepared(statement, queryResult);
        if (!status.IsSuccess())
        {
            var errorMessage = NativeMethods.Query.DuckDBResultError(queryResult).ToManagedString(false);
            queryResult.Dispose();
            throw new DuckDBException(string.IsNullOrEmpty(errorMessage) ? "DuckDBQuery failed" : errorMessage, status);
        }

        return queryResult;
    }

    private static void BindParameters(DuckDBPreparedStatement preparedStatement, DuckDBParameterCollection parameterCollection)
    {
        var expectedParameters = NativeMethods.PreparedStatements.DuckDBParams(preparedStatement);
        if (expectedParameters != parameterCollection.Count)
        {
            throw new InvalidOperationException($"Invalid number of parameters. Expected {expectedParameters}, got {parameterCollection.Count}");
        }

        for (var i = 0; i < parameterCollection.Count; ++i)
        {
            var param = parameterCollection[i];
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

        if (!Binders.TryGetValue(parameter.DbType, out var binder))
        {
            throw new InvalidOperationException($"Unable to bind value of type {parameter.DbType}.");
        }

        var result = binder(preparedStatement, index, parameter.Value);
        if (!result.IsSuccess())
        {
            var errorMessage = NativeMethods.PreparedStatements.DuckDBPrepareError(preparedStatement).ToManagedString(false);
            throw new InvalidOperationException($"Unable to bind parameter {index}: {errorMessage}");
        }
    }

    private static DuckDBState BindBoolean(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindBoolean(preparedStatement, index, (bool)value);

    private static DuckDBState BindInt8(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindInt8(preparedStatement, index, (sbyte)value);

    private static DuckDBState BindInt16(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindInt16(preparedStatement, index, (short)value);

    private static DuckDBState BindInt32(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindInt32(preparedStatement, index, (int)value);

    private static DuckDBState BindInt64(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindInt64(preparedStatement, index, (long)value);

    private static DuckDBState BindFloat(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindFloat(preparedStatement, index, (float)value);

    private static DuckDBState BindDouble(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindDouble(preparedStatement, index, (double)value);

    private static DuckDBState BindString(DuckDBPreparedStatement preparedStatement, long index, object value)
    {
        using var unmanagedString = ((string)value).ToUnmanagedString();
        return NativeMethods.PreparedStatements.DuckDBBindVarchar(preparedStatement, index, unmanagedString);
    }

    public void Dispose()
    {
        statement?.Dispose();
    }
}