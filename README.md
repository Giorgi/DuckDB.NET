# DuckDB.NET

[DuckDB](https://duckdb.org/) bindings for C#

Note: The library is in very early stage and contributions are more than wellcome.

## Usage

There are two ways to work with DuckDB from C# :

Using ADO.NET Provider or using low level bindings library for DuckDB. The ADO.NET Provider is built on top of the low level library and is the recommended and most straightforward way for working with DuckDB.

### Using ADO.NET Provider

```cs
using (var duckDBConnection = new DuckDBConnection("Data Source=file.db"))
{
  duckDBConnection.Open();

  var command = duckDBConnection.CreateCommand();

  command.CommandText = "CREATE TABLE integers(foo INTEGER, bar INTEGER);";
  var executeNonQuery = command.ExecuteNonQuery();

  command.CommandText = "INSERT INTO integers VALUES (3, 4), (5, 6), (7, 8);";
  executeNonQuery = command.ExecuteNonQuery();

  command.CommandText = "Select count(*) from integers";
  var executeScalar = command.ExecuteScalar();

  command.CommandText = "SELECT foo, bar FROM integers";
  var reader = command.ExecuteReader();

  PrintQueryResults(reader);
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
      var val = queryResult.GetInt32(ordinal);
      Console.Write(val);
      Console.Write(" ");
    }

    Console.WriteLine();
  }
}
```

For in-memory database use `Data Source=:memory:` connection string.

### Use low level bindings library

```cs
var result = DuckDBOpen(null, out var database);

using (database)
{
  result = DuckDBConnect(database, out var connection);
  using (connection)
  {
    result = DuckDBQuery(connection, "CREATE TABLE integers(foo INTEGER, bar INTEGER);", out var queryResult);
    result = DuckDBQuery(connection, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, 8);", out queryResult);
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

private static void PrintQueryResults(DuckDBResult queryResult)
{
  for (var index = 0; index < queryResult.Columns.Count; index++)
  {
    var column = queryResult.Columns[index];
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

```
