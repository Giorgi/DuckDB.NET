# DuckDB.NET

[DuckDB](https://duckdb.org/) bindings for C#

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/Giorgi/DuckDB.NET/ci.yml?branch=main&logo=GitHub&style=for-the-badge)](https://github.com/Giorgi/DuckDB.NET/actions/workflows/ci.yml)
[![Coveralls](https://img.shields.io/coveralls/github/Giorgi/DuckDB.NET?logo=coveralls&style=for-the-badge)](https://coveralls.io/github/Giorgi/DuckDB.NET)
[![License](https://img.shields.io/badge/License-Mit-blue.svg?style=for-the-badge&logo=mit)](LICENSE.md)
[![Ko-Fi](https://img.shields.io/static/v1?style=for-the-badge&message=Support%20the%20Project&color=success&logo=ko-fi&label=$$)](https://ko-fi.com/U6U81LHU8)
[![Discord](https://img.shields.io/badge/DuckDB-.Net-%23FFF000?logo=DuckDB&style=for-the-badge)](https://discord.com/channels/909674491309850675/1051088721996427265)

[![NuGet DuckDB.NET.Data](https://img.shields.io/nuget/dt/DuckDB.NET.Data.svg?label=DuckDB.NET.Data&style=for-the-badge&logo=NuGet)](https://www.nuget.org/packages/DuckDB.NET.Data/)
[![NuGet DuckDB.NET.Bindings](https://img.shields.io/nuget/dt/DuckDB.NET.Bindings.svg?label=DuckDB.NET.Bindings&style=for-the-badge&logo=NuGet)](https://www.nuget.org/packages/DuckDB.NET.Bindings/)

[![NuGet DuckDB.NET.Data.Full](https://img.shields.io/nuget/dt/DuckDB.NET.Data.Full.svg?label=DuckDB.NET.Data.Full&style=for-the-badge&logo=NuGet)](https://www.nuget.org/packages/DuckDB.NET.Data.Full/)
[![NuGet DuckDB.NET.Bindings.Full](https://img.shields.io/nuget/dt/DuckDB.NET.Bindings.Full.svg?label=DuckDB.NET.Bindings.Full&style=for-the-badge&logo=NuGet)](https://www.nuget.org/packages/DuckDB.NET.Bindings.Full/)

![Project Icon](https://raw.githubusercontent.com/Giorgi/DuckDB.NET/main/Logo.jpg "DuckDB.NET Project Icon")

## Usage

```sh
dotnet add package DuckDB.NET.Data.Full
```

```cs
using (var duckDBConnection = new DuckDBConnection("Data Source=file.db"))
{
  duckDBConnection.Open();

  using var command = duckDBConnection.CreateCommand();

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
## Known Issues

When debugging your project that uses DuckDB.NET library, you may get the following error: **System.AccessViolationException: Attempted to read or write protected memory. This is often an indication that other memory is corrupt**. The error happens due to debugger interaction with the native memory. For a workaround check out [Debugger Options mess up debugging session during Marshalling
](https://youtrack.jetbrains.com/issue/RIDER-114126/Debugger-Options-mess-up-debugging-session-during-Marshalling)

## Documentation

Documentation is available at [https://duckdb.net](https://duckdb.net)

## Support

If you encounter a bug with the library [Create an Issue](https://github.com/Giorgi/DuckDB.NET/issues/new). Join the [DuckDB `dotnet` channel](https://discord.duckdb.org/) for DuckDB.NET-related topics.

## Contributors

[![Contributors](https://contrib.rocks/image?repo=Giorgi/DuckDB.NET)](https://github.com/Giorgi/DuckDB.NET/graphs/contributors)

## Sponsors

A big thanks to [DuckDB Labs](https://duckdblabs.com/) and [AWS Open Source Software Fund](https://github.com/aws/dotnet-foss) for sponsoring the project!

[![DuckDB Labs](https://raw.githubusercontent.com/Giorgi/DuckDB.NET/main/.github/sponsors/duckdb-labs-logo.png)](https://duckdblabs.com/)

[![AWS](https://raw.githubusercontent.com/Giorgi/DuckDB.NET/main/.github/sponsors/aws-logo-small.png)](https://github.com/aws/dotnet-foss)
