using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Numerics;
using System.Runtime.ExceptionServices;
using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Data;

internal sealed class PreparedStatement : IDisposable
{
    private static readonly Dictionary<DbType, Func<DuckDBPreparedStatement, long, object, DuckDBState>> Binders = new()
    {
        { DbType.Guid, BindObject },
        { DbType.Currency, BindObject },
        { DbType.Boolean, BindBoolean },
        { DbType.SByte, BindInt8 },
        { DbType.Int16, BindInt16 },
        { DbType.Int32, BindInt32 },
        { DbType.Int64, BindInt64 },
        { DbType.Byte, BindUInt8 },
        { DbType.UInt16, BindUInt16 },
        { DbType.UInt32, BindUInt32 },
        { DbType.UInt64, BindUInt64 },
        { DbType.Single, BindFloat },
        { DbType.Double, BindDouble },
        { DbType.String, BindString },
        { DbType.VarNumeric, BindHugeInt },
        { DbType.Binary, BindBlob },
        { DbType.Date, BindDateOnly },
        { DbType.Time, BindTimeOnly },
        { DbType.DateTime, BindTimestamp }
    };

    private readonly DuckDBPreparedStatement statement;

    private PreparedStatement(DuckDBPreparedStatement statement)
    {
        this.statement = statement;
    }

    public static PreparedStatement PrepareSingle(DuckDBNativeConnection connection, string query)
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

    public static (List<PreparedStatement> statements, List<DuckDBResult> results) PrepareMultiple(DuckDBNativeConnection connection, string query, DuckDBParameterCollection parameters)
    {
        var statements = new List<PreparedStatement>();
        var results = new List<DuckDBResult>();

        var statementCount = NativeMethods.ExtractStatements.DuckDBExtractStatements(connection, query, out var extractedStatements);
        
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

                var preparedStatement = new PreparedStatement(statement);
                statements.Add(preparedStatement);

                if (status.IsSuccess())
                {
                    try
                    {
                        var result = preparedStatement.Execute(parameters);
                        results.Add(result);
                    }
                    catch (Exception ex)
                    {
                        var capture = ExceptionDispatchInfo.Capture(ex);

                        CleanUp();
                        
                        capture.Throw();
                    }
                }
                else
                {
                    var errorMessage = NativeMethods.PreparedStatements.DuckDBPrepareError(statement).ToManagedString(false);
                    
                    CleanUp();

                    throw new DuckDBException(string.IsNullOrEmpty(errorMessage) ? "DuckDBQuery failed" : errorMessage, status);
                }
            }
        }

        return (statements, results);

        void CleanUp()
        {
            foreach (var result in results)
            {
                result.Dispose();
            }

            foreach (var statement in statements)
            {
                statement.Dispose();
            }
        }
    }

    public DuckDBResult Execute(DuckDBParameterCollection parameterCollection)
    {
        BindParameters(statement, parameterCollection);

        var status = NativeMethods.PreparedStatements.DuckDBExecutePrepared(statement, out var queryResult);
        if (!status.IsSuccess())
        {
            var errorMessage = NativeMethods.Query.DuckDBResultError(ref queryResult).ToManagedString(false);
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
        if (parameter.Value.IsNull())
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

    private static DuckDBState BindObject(DuckDBPreparedStatement preparedStatement, long index, object value)
        => BindString(preparedStatement, index, Convert.ToString(value, CultureInfo.InvariantCulture));

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

    private static DuckDBState BindUInt8(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindUInt8(preparedStatement, index, (byte)value);

    private static DuckDBState BindUInt16(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindUInt16(preparedStatement, index, (ushort)value);

    private static DuckDBState BindUInt32(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindUInt32(preparedStatement, index, (uint)value);

    private static DuckDBState BindUInt64(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindUInt64(preparedStatement, index, (ulong)value);


    private static DuckDBState BindFloat(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindFloat(preparedStatement, index, (float)value);

    private static DuckDBState BindDouble(DuckDBPreparedStatement preparedStatement, long index, object value)
        => NativeMethods.PreparedStatements.DuckDBBindDouble(preparedStatement, index, (double)value);

    private static DuckDBState BindString(DuckDBPreparedStatement preparedStatement, long index, object value)
    {
        using var unmanagedString = ((string)value).ToUnmanagedString();
        return NativeMethods.PreparedStatements.DuckDBBindVarchar(preparedStatement, index, unmanagedString);
    }

    private static DuckDBState BindHugeInt(DuckDBPreparedStatement preparedStatement, long index, object value) =>
        NativeMethods.PreparedStatements.DuckDBBindHugeInt(preparedStatement, index, new DuckDBHugeInt((BigInteger)value));

    private static DuckDBState BindBlob(DuckDBPreparedStatement preparedStatement, long index, object value)
    {
        var bytes = (byte[])value;
        return NativeMethods.PreparedStatements.DuckDBBindBlob(preparedStatement, index, bytes, bytes.LongLength);
    }

    private static DuckDBState BindDateOnly(DuckDBPreparedStatement preparedStatement, long index, object value)
    {
        var date = NativeMethods.DateTime.DuckDBToDate((DuckDBDateOnly)value);
        return NativeMethods.PreparedStatements.DuckDBBindDate(preparedStatement, index, date);
    }

    private static DuckDBState BindTimeOnly(DuckDBPreparedStatement preparedStatement, long index, object value)
    {
        var time = NativeMethods.DateTime.DuckDBToTime((DuckDBTimeOnly)value);
        return NativeMethods.PreparedStatements.DuckDBBindTime(preparedStatement, index, time);
    }

    private static DuckDBState BindTimestamp(DuckDBPreparedStatement preparedStatement, long index, object value)
    {
        var timestamp = DuckDBTimestamp.FromDateTime((DateTime)value);
        var timestampStruct = NativeMethods.DateTime.DuckDBToTimestamp(timestamp);
        return NativeMethods.PreparedStatements.DuckDBBindTimestamp(preparedStatement, index, timestampStruct);
    }

    public void Dispose()
    {
        statement.Dispose();
    }
}