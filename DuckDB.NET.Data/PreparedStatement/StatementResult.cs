namespace DuckDB.NET.Data.PreparedStatement;

// The result of one statement, with the type of the statement. Since DuckDB 2.0 the result of a prepared statement
// reports EXECUTE as its type (duckdb_result_statement_type), so the type is taken from the prepared statement.
internal readonly record struct StatementResult(DuckDBResult Result, DuckDBStatementType StatementType);
