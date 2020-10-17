using System;
using static DuckDB.NET.NativeMethods;

namespace DuckDB.NET.Samples
{
    class Program
    {
        static void Main(string[] args)
        {
            var result = DuckDBOpen(null, out var database);
            result = DuckDBConnect(database, out var connection);
            result = DuckDBQuery(connection, "CREATE TABLE integers(foo INTEGER, bar INTEGER);", out var queryResult);
            result = DuckDBQuery(connection, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", out queryResult);
            result = DuckDBQuery(connection, "SELECT foo, bar FROM integers", out queryResult);

            PrintQueryResults(queryResult);

            result = DuckDBPrepare(connection, "INSERT INTO integers VALUES (?, ?)", out var statement);

            result = DuckDBBindInt32(statement, 1, 42); // the parameter index starts counting at 1!
            result = DuckDBBindInt32(statement, 2, 43);

            result = DuckDBExecutePrepared(statement, out var _);
            DuckDBDestroyPrepare(out statement);

            result = DuckDBPrepare(connection, "SELECT * FROM integers WHERE foo = ?", out statement);
            result = DuckDBBindInt32(statement, 1, 42);
            result = DuckDBExecutePrepared(statement, out queryResult);

            PrintQueryResults(queryResult);

            // clean up
            DuckDBDestroyResult(out queryResult);
            DuckDBDestroyPrepare(out statement);

            DuckDBDisconnect(out connection);
            DuckDBClose(out database);
        }

        private static void PrintQueryResults(DuckDBResult queryResult)
        {
            foreach (var column in queryResult.Columns)
            {
                Console.Write($"{column.Name} ");
            }
            Console.WriteLine();

            for (long row = 0; row < queryResult.RowCount; row++)
            {
                for (long column = 0; column < queryResult.ColumnCount; column++)
                {
                    var val = DuckDBValueInt32(queryResult, column, row);
                    Console.Write(val);
                    Console.Write(" ");
                }

                Console.WriteLine();
            }
        }
    }
}
