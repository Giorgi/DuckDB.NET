using Bogus;
using DuckDB.NET.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
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
    protected DuckDBConnection Connection { get; }

    protected Faker Faker { get; init; } = new Faker();

    protected List<T> GetRandomList<T>(Func<Faker, T> generator, int? count = 20)
    {
        return Enumerable.Range(0, count ?? Faker.Random.Int(0, 50)).Select(i => generator(Faker)).ToList();
    }

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