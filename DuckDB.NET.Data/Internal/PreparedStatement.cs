using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Numerics;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data.Internal;

internal sealed class PreparedStatement : IDisposable
{
    private static readonly Dictionary<DbType, Func<object, DuckDBValue>> ValueCreators = new()
    {
        { DbType.Guid, value =>
            {
                using var handle = value.ToString().ToUnmanagedString();
                return NativeMethods.Value.DuckDBCreateVarchar(handle);
            }
        },
        { DbType.Currency, value =>
            {
                using var handle = ((decimal)value).ToString(CultureInfo.InvariantCulture).ToUnmanagedString();
                return NativeMethods.Value.DuckDBCreateVarchar(handle);
            }
        },
        { DbType.Boolean, value => NativeMethods.Value.DuckDBCreateBool((bool)value) },
        { DbType.SByte, value => NativeMethods.Value.DuckDBCreateInt8((sbyte)value) },
        { DbType.Int16, value => NativeMethods.Value.DuckDBCreateInt16((short)value) },
        { DbType.Int32, value => NativeMethods.Value.DuckDBCreateInt32((int)value) },
        { DbType.Int64, value => NativeMethods.Value.DuckDBCreateInt64((long)value) },
        { DbType.Byte, value => NativeMethods.Value.DuckDBCreateUInt8((byte)value) },
        { DbType.UInt16, value => NativeMethods.Value.DuckDBCreateUInt16((ushort)value) },
        { DbType.UInt32, value => NativeMethods.Value.DuckDBCreateUInt32((uint)value) },
        { DbType.UInt64, value => NativeMethods.Value.DuckDBCreateUInt64((ulong)value) },
        { DbType.Single, value => NativeMethods.Value.DuckDBCreateFloat((float)value) },
        { DbType.Double, value => NativeMethods.Value.DuckDBCreateDouble((double)value) },
        { DbType.String, value =>
            {
                using var handle = ((string)value).ToUnmanagedString();
                return NativeMethods.Value.DuckDBCreateVarchar(handle);
            }
        },
        { DbType.VarNumeric, value => NativeMethods.Value.DuckDBCreateHugeInt(new((BigInteger)value)) },
        { DbType.Binary, value =>
            {
                var bytes = (byte[])value;
                return NativeMethods.Value.DuckDBCreateBlob(bytes, bytes.Length);
            }
        },
        { DbType.Date, value =>
            {
#if NET6_0_OR_GREATER
                var date = NativeMethods.DateTimeHelpers.DuckDBToDate(value is DateOnly dateOnly ? (DuckDBDateOnly)dateOnly : (DuckDBDateOnly)value);
#else
                var date = NativeMethods.DateTimeHelpers.DuckDBToDate((DuckDBDateOnly)value);
#endif
                return NativeMethods.Value.DuckDBCreateDate(date);
            }
        },
        { DbType.Time, value =>
            {
#if NET6_0_OR_GREATER
                var time = NativeMethods.DateTimeHelpers.DuckDBToTime(value is TimeOnly timeOnly ? (DuckDBTimeOnly)timeOnly : (DuckDBTimeOnly)value);
#else
                var time = NativeMethods.DateTimeHelpers.DuckDBToTime((DuckDBTimeOnly)value);
#endif
                return NativeMethods.Value.DuckDBCreateTime(time);
            }
        },
        { DbType.DateTime, value =>
            {
                var timestamp = DuckDBTimestamp.FromDateTime((DateTime)value);
                var timestampStruct = NativeMethods.DateTimeHelpers.DuckDBToTimestamp(timestamp);
                return NativeMethods.Value.DuckDBCreateTimestamp(timestampStruct);
            }
        },
    };

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

    public DuckDBResult Execute(DuckDBParameterCollection parameterCollection, bool useStreamingMode)
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
        if (parameter.Value.IsNull())
        {
            NativeMethods.PreparedStatements.DuckDBBindNull(preparedStatement, index);
            return;
        }

        // if (!ValueCreators.TryGetValue(parameter.DbType, out var func))
        // {
        //     throw new InvalidOperationException($"Unable to bind value of type {parameter.DbType}.");
        // }

        var duckDBValue2 = parameter.Value!.ToDuckDBValue();

        //using var duckDBValue = func(parameter.Value!);
        var result = NativeMethods.PreparedStatements.DuckDBBindValue(preparedStatement, index, duckDBValue2);

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