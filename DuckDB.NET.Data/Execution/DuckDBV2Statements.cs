using DuckDB.NET.Data.PreparedStatement;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Execution;

/// <summary>Runs a command's text through the C API V2: parse, then execute one statement at a time.</summary>
internal static unsafe class DuckDBV2Statements
{
    /// <summary>
    /// The results of the statements in <paramref name="sql"/>, one per <c>MoveNext()</c>: a later statement can
    /// depend on an earlier one having run, so each runs only when the previous result is done. V2 allows one open
    /// result per connection, so the caller finishes or disposes each result before moving on.
    /// </summary>
    public static IEnumerable<DuckDBV2StatementResult> Execute(DuckDBV2Connection connection, string sql, DuckDBParameterCollection parameters)
    {
        using var iterator = Parse(connection, sql);

        var any = false;

        while (Next(iterator) is { } statement)
        {
            any = true;

            DuckDBV2StatementResult result;
            using (statement)
            {
                result = ExecuteStatement(connection, statement, parameters);
            }

            yield return result;
        }

        if (!any)
        {
            throw new DuckDBException("The command text holds no statement to execute");
        }
    }

    /// <summary>
    /// Prepares a command that holds exactly one statement, to be executed many times. A command of several
    /// statements returns null: a later statement can depend on an earlier one having run, so each runs unprepared.
    /// </summary>
    public static PreparedStatementEntry? PrepareSingle(DuckDBV2Connection connection, string sql)
    {
        using var iterator = Parse(connection, sql);
        using var statement = Next(iterator) ?? throw new DuckDBException("The command text holds no statement to prepare");

        using (var another = Next(iterator))
        {
            if (another is not null)
            {
                return null;
            }
        }

        using var connectionLease = connection.Acquire();
        using var statementLease = statement.Acquire();

        _duckdb_v2_error_info* error = null;
        DUCKDB_V2_STATEMENT_TYPE statementType;
        NativeMethodsV2.duckdb_v2_sql_statement_get_type(statementLease.Pointer, &statementType, &error).ThrowIfFailed(ref error);

        ulong parameterCount;
        NativeMethodsV2.duckdb_v2_sql_statement_get_parameter_count(statementLease.Pointer, &parameterCount, &error).ThrowIfFailed(ref error);

        _duckdb_v2_prepared_statement* prepared;
        var code = NativeMethodsV2.duckdb_v2_prepared_statement_create(connectionLease.Pointer, statementLease.Pointer, 0, &prepared, &error);

        try
        {
            code.ThrowIfFailed(ref error, "Preparing the statement failed");
        }
        catch (DuckDBException exception) when (exception.NativeErrorCode == DuckDBErrorCode.ResourceInUse)
        {
            throw OpenReader(exception);
        }

        var handle = new DuckDBV2PreparedStatement(prepared);

        try
        {
            // A statement DuckDB expands into several (a dynamic PIVOT) takes no parameters, and its shape is known only
            // when it runs.
            var parameters = parameterCount == 0 ? null : ReadParameters(connection, statement);
            return new PreparedStatementEntry(handle, statementType.ToStatementType(), parameters);
        }
        catch
        {
            handle.Dispose();
            throw;
        }
    }

    private static DuckDBV2StatementIterator Parse(DuckDBV2Connection connection, string sql)
    {
        using var text = DuckDBV2Utf8.FromManaged(sql);
        using var lease = connection.Acquire();

        fixed (byte* sqlPointer = text.Bytes)
        {
            _duckdb_v2_error_info* error = null;
            _duckdb_v2_statement_iterator* iterator;

            // A parse error invalidates the active transaction, as an error from running the statement would: the
            // statements are parsed to be run.
            NativeMethodsV2.duckdb_v2_parse_sql(lease.Pointer, (sbyte*)sqlPointer, (ulong)text.Length, 1, &iterator, &error)
                           .ThrowIfFailed(ref error, "Parsing the command text failed");

            return new DuckDBV2StatementIterator(iterator);
        }
    }

    // V2 parses lazily: a syntax error is reported here, for the statement it is in.
    private static DuckDBV2SqlStatement? Next(DuckDBV2StatementIterator iterator)
    {
        using var lease = iterator.Acquire();
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_sql_statement* statement;
        NativeMethodsV2.duckdb_v2_statement_iterator_next(lease.Pointer, &statement, &error).ThrowIfFailed(ref error, "Parsing the command text failed");

        return statement == null ? null : new DuckDBV2SqlStatement(statement);
    }

    private static DuckDBV2StatementResult ExecuteStatement(DuckDBV2Connection connection, DuckDBV2SqlStatement statement, DuckDBParameterCollection parameters)
    {
        using var connectionLease = connection.Acquire();
        using var statementLease = statement.Acquire();
        var connectionPointer = connectionLease.Pointer;
        var statementPointer = statementLease.Pointer;

        _duckdb_v2_error_info* error = null;
        DUCKDB_V2_STATEMENT_TYPE statementType;
        NativeMethodsV2.duckdb_v2_sql_statement_get_type(statementPointer, &statementType, &error).ThrowIfFailed(ref error);

        // The parser counts the parameters; their types need a bind, which a statement without any does not pay for.
        // Values for a statement that declares none are ignored, as with V1.
        ulong parameterCount;
        NativeMethodsV2.duckdb_v2_sql_statement_get_parameter_count(statementPointer, &parameterCount, &error).ThrowIfFailed(ref error);

        _duckdb_v2_result* result;
        DUCKDB_V2_ERROR code;

        if (parameterCount == 0)
        {
            code = NativeMethodsV2.duckdb_v2_statement_execute(connectionPointer, statementPointer, null, null, null, 0, &result, &error);
        }
        else
        {
            using var parameterInfo = ReadParameters(connection, statement);
            code = parameterInfo.Execute(connection, parameters,
                                         (names, lengths, values, count, output, executeError) =>
                                             NativeMethodsV2.duckdb_v2_statement_execute(connectionPointer, statementPointer, names, lengths, values, count, output, executeError),
                                         &result, &error);
        }

        return Wrap(code, result, ref error, statementType.ToStatementType());
    }

    private static DuckDBV2Parameters ReadParameters(DuckDBV2Connection connection, DuckDBV2SqlStatement statement)
    {
        try
        {
            return DuckDBV2Parameters.Read(connection, statement);
        }
        catch (DuckDBException exception) when (exception.NativeErrorCode == DuckDBErrorCode.ResourceInUse)
        {
            throw OpenReader(exception);
        }
    }

    /// <summary>The result of an execution, or the exception for its failure.</summary>
    internal static DuckDBV2StatementResult Wrap(DUCKDB_V2_ERROR code, _duckdb_v2_result* result, ref _duckdb_v2_error_info* error, DuckDBStatementType statementType)
    {
        try
        {
            code.ThrowIfFailed(ref error, "Executing the statement failed");
        }
        catch (DuckDBException exception) when (exception.NativeErrorCode == DuckDBErrorCode.ResourceInUse)
        {
            throw OpenReader(exception);
        }

        var handle = new DuckDBV2Result(result);

        try
        {
            return new DuckDBV2StatementResult(handle, statementType);
        }
        catch
        {
            handle.Dispose();
            throw;
        }
    }

    // V2 allows one open result per connection, and every result streams.
    internal static InvalidOperationException OpenReader(DuckDBException exception) =>
        new("The connection has an open data reader. Read it to the end or close it before executing another command on the connection.", exception);
}
