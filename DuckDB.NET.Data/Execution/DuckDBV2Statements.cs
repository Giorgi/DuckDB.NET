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
    public static IEnumerable<DuckDBV2StatementResult> Execute(DuckDBV2Connection connection, string sql)
    {
        using var iterator = Parse(connection, sql);

        var any = false;

        while (Next(iterator) is { } statement)
        {
            any = true;

            DuckDBV2StatementResult result;
            using (statement)
            {
                result = ExecuteStatement(connection, statement);
            }

            yield return result;
        }

        if (!any)
        {
            throw new DuckDBException("The command text holds no statement to execute");
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

    private static DuckDBV2StatementResult ExecuteStatement(DuckDBV2Connection connection, DuckDBV2SqlStatement statement)
    {
        using var connectionLease = connection.Acquire();
        using var statementLease = statement.Acquire();

        _duckdb_v2_error_info* error = null;
        DUCKDB_V2_STATEMENT_TYPE statementType;
        NativeMethodsV2.duckdb_v2_sql_statement_get_type(statementLease.Pointer, &statementType, &error).ThrowIfFailed(ref error);

        _duckdb_v2_result* result;
        var code = NativeMethodsV2.duckdb_v2_statement_execute(connectionLease.Pointer, statementLease.Pointer, null, null, null, 0, &result, &error);

        try
        {
            code.ThrowIfFailed(ref error, "Executing the statement failed");
        }
        catch (DuckDBException exception) when (exception.NativeErrorCode == DuckDBErrorCode.ResourceInUse)
        {
            // V2 allows one open result per connection, and every result streams.
            throw new InvalidOperationException("The connection has an open data reader. Read it to the end or close it before " +
                                                "executing another command on the connection.", exception);
        }

        var handle = new DuckDBV2Result(result);

        try
        {
            return new DuckDBV2StatementResult(handle, statementType.ToStatementType());
        }
        catch
        {
            handle.Dispose();
            throw;
        }
    }
}
