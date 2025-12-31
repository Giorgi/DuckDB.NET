using DuckDB.NET.Data.Mapping;

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

    // AppenderMap for Person - matches the example from the comment
    public class PersonMap : DuckDBAppenderMap<Person>
    {
        public PersonMap()
        {
            Map(p => p.Id);
            Map(p => p.Name);
            Map(p => p.Height);
            Map(p => p.BirthDate);
        }
    }

    [Fact]
    public void MappedAppender_ValidatesTypeMatching()
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

        // Use mapped appender - types are validated at creation
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

    // Example with type mismatch - should throw
    public class WrongTypeMap : DuckDBAppenderMap<Person>
    {
        public WrongTypeMap()
        {
            Map(p => p.Id);
            Map(p => p.Name);
            Map(p => p.BirthDate);  // DateTime mapped to column 2, but column 2 is REAL
            Map(p => p.Height);
        }
    }

    [Fact]
    public void MappedAppender_ThrowsOnTypeMismatch()
    {
        Command.CommandText = "CREATE TABLE person_mismatch(id INTEGER, name VARCHAR, height REAL, birth_date TIMESTAMP);";
        Command.ExecuteNonQuery();

        // Should throw when creating the appender due to type mismatch
        Connection.Invoking(conn =>
        {
            var appender = conn.CreateAppender<Person, WrongTypeMap>("person_mismatch");
        }).Should().Throw<InvalidOperationException>()
          .WithMessage("*Type mismatch*");
    }

    // Example with DefaultValue and NullValue
    public class PersonWithDefaults
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    public class PersonWithDefaultsMap : DuckDBAppenderMap<PersonWithDefaults>
    {
        public PersonWithDefaultsMap()
        {
            Map(p => p.Id);
            Map(p => p.Name);
            DefaultValue();  // Use default for column 2
            NullValue();     // Use null for column 3
        }
    }

    [Fact]
    public void MappedAppender_SupportsDefaultAndNull()
    {
        Command.CommandText = "CREATE TABLE person_defaults(id INTEGER, name VARCHAR, age INT DEFAULT 18, city VARCHAR);";
        Command.ExecuteNonQuery();

        var people = new[]
        {
            new PersonWithDefaults { Id = 1, Name = "Alice" },
            new PersonWithDefaults { Id = 2, Name = "Bob" },
        };

        using (var appender = Connection.CreateAppender<PersonWithDefaults, PersonWithDefaultsMap>("person_defaults"))
        {
            appender.AppendRecords(people);
        }

        Command.CommandText = "SELECT id, name, age, city FROM person_defaults";
        using var reader = Command.ExecuteReader();
        
        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);
        reader.GetString(1).Should().Be("Alice");
        reader.GetInt32(2).Should().Be(18);
        reader.IsDBNull(3).Should().BeTrue();
    }
}
