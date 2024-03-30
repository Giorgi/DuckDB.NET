using System;
using System.Data;
using DuckDB.NET.Data;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBDatabaseFixture : IDisposable
{
    public DuckDBDatabaseFixture()
    {
        Connection = new DuckDBConnection("DataSource=:memory:");
        Connection.Open();
    }

    public void Dispose()
    {
        Connection.Dispose();
    }

    internal DuckDBConnection Connection { get; }
}

public class DuckDBTestBase : IDisposable, IClassFixture<DuckDBDatabaseFixture>
{
    protected DuckDBCommand Command { get; }
    internal DuckDBConnection Connection { get; }

    public DuckDBTestBase(DuckDBDatabaseFixture db)
    {
        Connection = db.Connection;

        if (Connection.State == ConnectionState.Closed)
        {
            Connection.Open();
        }

        Command = db.Connection.CreateCommand();
    }

    public virtual void Dispose()
    {
        Command?.Dispose();
    }
}