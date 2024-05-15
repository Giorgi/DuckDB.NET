using Dapper;
using DuckDB.NET.Data;
using DuckDB.NET.Native;
using DuckDB.NET.Test.Helpers;
using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using static DuckDB.NET.Native.NativeMethods;

namespace DuckDB.NET.Samples
{
    class Program
    {
        static void Main(string[] args)
        {
            if (!NativeLibraryHelper.TryLoad())
            {
                Console.Error.WriteLine("native assembly not found");
                return;
            }

            DapperSample();

            AdoNetSamples();

            LowLevelBindingsSample();
        }

        private static void DapperSample()
        {
            var connectionString = "Data Source=:memory:";
            using (var cn = new DuckDBConnection(connectionString))
            {
                cn.Open();

                Console.WriteLine("DuckDB version: {0}", cn.ServerVersion);

                cn.Execute("CREATE TABLE test (id INTEGER, name VARCHAR)");

                var query = cn.Query<Row>("SELECT * FROM test");
                Console.WriteLine("Initial count: {0}", query.Count());

                cn.Execute("INSERT INTO test (id,name) VALUES (123,'test')");

                query = cn.Query<Row>("SELECT * FROM test");

                foreach (var q in query)
                {
                    Console.WriteLine($"{q.Id} {q.Name}");
                }
            }
        }

        private static void AdoNetSamples()
        {
            if (File.Exists("file.db"))
            {
                File.Delete("file.db");
            }

            using var duckDBConnection = new DuckDBConnection("Data Source=file.db");
            duckDBConnection.Open();

            using var command = duckDBConnection.CreateCommand();
            command.CommandText = "CREATE TABLE integers(foo INTEGER, bar INTEGER);";
            var executeNonQuery = command.ExecuteNonQuery();

            command.CommandText = "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);";
            executeNonQuery = command.ExecuteNonQuery();

            command.CommandText = "Select count(*) from integers";
            var executeScalar = command.ExecuteScalar();

            command.CommandText = "SELECT foo, bar FROM integers";
            var reader = command.ExecuteReader();
            PrintQueryResults(reader);

            var results = duckDBConnection.Query<FooBar>("SELECT foo, bar FROM integers");

            try
            {
                command.CommandText = "Not a valid Sql statement";
                var causesError = command.ExecuteNonQuery();
            }
            catch (DuckDBException e)
            {
                Console.WriteLine(e.Message);
            }
        }

        private static void LowLevelBindingsSample()
        {
            var result = Startup.DuckDBOpen(null, out var database);

            using (database)
            {
                result = Startup.DuckDBConnect(database, out var connection);
                using (connection)
                {
                    result = Query.DuckDBQuery(connection, "CREATE TABLE integers(foo INTEGER, bar INTEGER);", out _);
                    result = Query.DuckDBQuery(connection, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", out _);
                    result = Query.DuckDBQuery(connection, "SELECT foo, bar FROM integers", out var queryResult);

                    PrintQueryResults(queryResult);

                    // clean up
                    Query.DuckDBDestroyResult(ref queryResult);

                    result = PreparedStatements.DuckDBPrepare(connection, "INSERT INTO integers VALUES (?, ?)", out var insertStatement);

                    using (insertStatement)
                    {
                        result = PreparedStatements.DuckDBBindInt32(insertStatement, 1, 42); // the parameter index starts counting at 1!
                        result = PreparedStatements.DuckDBBindInt32(insertStatement, 2, 43);

                        result = PreparedStatements.DuckDBExecutePrepared(insertStatement, out _);
                    }


                    result = PreparedStatements.DuckDBPrepare(connection, "SELECT * FROM integers WHERE foo = ?", out var selectStatement);

                    using (selectStatement)
                    {
                        result = PreparedStatements.DuckDBBindInt32(selectStatement, 1, 42);

                        result = PreparedStatements.DuckDBExecutePrepared(selectStatement, out queryResult);
                    }

                    PrintQueryResults(queryResult);

                    // clean up
                    Query.DuckDBDestroyResult(ref queryResult);
                }
            }
        }

        private static void PrintQueryResults(DbDataReader queryResult)
        {
            for (var index = 0; index < queryResult.FieldCount; index++)
            {
                var column = queryResult.GetName(index);
                Console.Write($"{column} ");
            }

            Console.WriteLine();
            
            while (queryResult.Read())
            {
                for (int ordinal = 0; ordinal < queryResult.FieldCount; ordinal++)
                {
                    if (queryResult.IsDBNull(ordinal))
                    {
                        Console.WriteLine("NULL");
                        continue;
                    }
                    var val = queryResult.GetValue(ordinal);
                    Console.Write(val);
                    Console.Write(" ");
                }

                Console.WriteLine();
            }
        }

        private static void PrintQueryResults(DuckDBResult queryResult)
        {
            var columnCount = (int)Query.DuckDBColumnCount(ref queryResult);
            for (var index = 0; index < columnCount; index++)
            {
                var columnName = Query.DuckDBColumnName(ref queryResult, index).ToManagedString(false);
                Console.Write($"{columnName} ");
            }

            Console.WriteLine();

            var rowCount = Query.DuckDBRowCount(ref queryResult);
            for (long row = 0; row < rowCount; row++)
            {
                for (long column = 0; column < columnCount; column++)
                {
                    var val = Types.DuckDBValueInt32(ref queryResult, column, row);
                    Console.Write(val);
                    Console.Write(" ");
                }

                Console.WriteLine();
            }
        }
    }

    class FooBar
    {
        public int Foo { get; set; }
        public int Bar { get; set; }
    }

    class Row
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }
}
