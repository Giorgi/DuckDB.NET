using System.Data;

namespace DuckDB.NET.Data.Extensions;

internal static class DbConnectionExtension
{
    public static int ExecuteNonQuery(this IDbConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteNonQuery();
    }

    public static bool IsOpen(this IDbConnection connection) => connection.State == ConnectionState.Open;
}