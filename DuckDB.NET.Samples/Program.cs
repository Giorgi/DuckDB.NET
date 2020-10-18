using System;
using System.Diagnostics;
using static DuckDB.NET.NativeMethods;

namespace DuckDB.NET.Samples
{
    class Program
    {
        static void Main(string[] args)
        {
            var result = DuckDBOpen(null, out var database);
            
            using (database)
            {
                result = DuckDBConnect(database, out var connection);
                using (connection)
                {
                    result = DuckDBQuery(connection, "CREATE TABLE integers(foo INTEGER, bar INTEGER);", out var queryResult);
                    result = DuckDBQuery(connection, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", out queryResult);
                    result = DuckDBQuery(connection, "SELECT foo, bar FROM integers", out queryResult);

                    PrintQueryResults(queryResult);

                    result = DuckDBPrepare(connection, "INSERT INTO integers VALUES (?, ?)", out var insertStatement);

                    using (insertStatement)
                    {
                        result = DuckDBBindInt32(insertStatement, 1, 42); // the parameter index starts counting at 1!
                        result = DuckDBBindInt32(insertStatement, 2, 43);

                        result = DuckDBExecutePrepared(insertStatement, out var _); 
                    }


                    result = DuckDBPrepare(connection, "SELECT * FROM integers WHERE foo = ?", out var selectStatement);
                    
                    using (selectStatement)
                    {
                        result = DuckDBBindInt32(selectStatement, 1, 42);

                        result = DuckDBExecutePrepared(selectStatement, out queryResult); 
                    }

                    PrintQueryResults(queryResult);

                    // clean up
                    DuckDBDestroyResult(out queryResult);
                }
            }
        }

        private static void PrintQueryResults(DuckDBResult queryResult)
        {
            for (var index = 0; index < queryResult.Columns.Count; index++)
            {
                var column = queryResult.Columns[index];
                Console.Write($"{column.Name} ");
                
                Debug.Assert(column.Name == DuckDBColumnName(queryResult, index));
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
