using DuckDB.NET.Data;
using DuckDB.NET.Data.Mapping;
using DuckDB.NET.Native;
using FluentAssertions;
using System;
using Xunit;

namespace DuckDB.NET.Test;

public class DuckDBMappedAppenderTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    // Example entity
    public class Person
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public float Height { get; set; }
        public DateTime BirthDate { get; set; }
    }

    // ClassMap for Person
    public class PersonMap : DuckDBClassMap<Person>
    {
        public PersonMap()
        {
            Map(p => p.Id).ToColumn(0);
            Map(p => p.Name).ToColumn(1);
            Map(p => p.Height).ToColumn(2);
            Map(p => p.BirthDate).ToColumn(3);
        }
    }

    [Fact]
    public void MappedAppender_PreventTypeMismatch()
    {
        // Create table with specific types
        Command.CommandText = "CREATE TABLE person(id INTEGER, name VARCHAR, height REAL, birth_date TIMESTAMP);";
        Command.ExecuteNonQuery();

        // Create records
        var people = new[]
        {
            new Person { Id = 1, Name = "Alice", Height = 1.65f, BirthDate = new DateTime(1990, 1, 15) },
            new Person { Id = 2, Name = "Bob", Height = 1.80f, BirthDate = new DateTime(1985, 5, 20) },
        };

        // Use mapped appender - types are enforced by the map
        using (var appender = Connection.CreateAppender<Person, PersonMap>("person"))
        {
            appender.AppendRecords(people);
        }

        // Verify data
        Command.CommandText = "SELECT * FROM person ORDER BY id";
        using var reader = Command.ExecuteReader();
        
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);
        reader.GetString(1).Should().Be("Alice");
        reader.GetFloat(2).Should().BeApproximately(1.65f, 0.01f);
        reader.GetDateTime(3).Should().Be(new DateTime(1990, 1, 15));

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(2);
        reader.GetString(1).Should().Be("Bob");
        reader.GetFloat(2).Should().BeApproximately(1.80f, 0.01f);
        reader.GetDateTime(3).Should().Be(new DateTime(1985, 5, 20));
    }

    [Fact]
    public void MappedAppender_SingleRecord()
    {
        Command.CommandText = "CREATE TABLE person_single(id INTEGER, name VARCHAR, height REAL, birth_date TIMESTAMP);";
        Command.ExecuteNonQuery();

        var person = new Person { Id = 1, Name = "Charlie", Height = 1.75f, BirthDate = new DateTime(1995, 3, 10) };

        using (var appender = Connection.CreateAppender<Person, PersonMap>("person_single"))
        {
            appender.AppendRecord(person);
        }

        Command.CommandText = "SELECT COUNT(*) FROM person_single";
        var count = (long)Command.ExecuteScalar()!;
        count.Should().Be(1);
    }

    [Fact]
    public void MappedAppender_WithNullValues()
    {
        // Entity with nullable properties
        Command.CommandText = "CREATE TABLE person_nullable(id INTEGER, name VARCHAR, height REAL, birth_date TIMESTAMP);";
        Command.ExecuteNonQuery();

        var people = new[]
        {
            new Person { Id = 1, Name = "Alice", Height = 1.65f, BirthDate = new DateTime(1990, 1, 15) },
            new Person { Id = 2, Name = null!, Height = 0, BirthDate = default },
        };

        using (var appender = Connection.CreateAppender<Person, PersonMap>("person_nullable"))
        {
            foreach (var p in people)
            {
                appender.AppendRecord(p);
            }
        }

        Command.CommandText = "SELECT COUNT(*) FROM person_nullable";
        var count = (long)Command.ExecuteScalar()!;
        count.Should().Be(2);
    }

    // Example with inferred types
    public class Product
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public double Price { get; set; }
    }

    public class ProductMap : DuckDBClassMap<Product>
    {
        public ProductMap()
        {
            // Types are automatically inferred
            Map(p => p.Id);
            Map(p => p.Name);
            Map(p => p.Price);
        }
    }

    [Fact]
    public void MappedAppender_InferredTypes()
    {
        Command.CommandText = "CREATE TABLE product(id INTEGER, name VARCHAR, price DOUBLE);";
        Command.ExecuteNonQuery();

        var products = new[]
        {
            new Product { Id = 1, Name = "Widget", Price = 9.99 },
            new Product { Id = 2, Name = "Gadget", Price = 19.99 },
        };

        using (var appender = Connection.CreateAppender<Product, ProductMap>("product"))
        {
            appender.AppendRecords(products);
        }

        Command.CommandText = "SELECT COUNT(*) FROM product";
        var count = (long)Command.ExecuteScalar()!;
        count.Should().Be(2);
    }
}
