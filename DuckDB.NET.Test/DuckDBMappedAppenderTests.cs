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

    // Example entity with a binary column
    public class FileEntry
    {
        public int Id { get; set; }
        public byte[] Data { get; set; } = Array.Empty<byte>();
    }

    public class FileEntryMap : DuckDBAppenderMap<FileEntry>
    {
        public FileEntryMap()
        {
            Map(f => f.Id);
            Map(f => f.Data);
        }
    }

    [Fact]
    public void MappedAppender_SupportsByteArray()
    {
        Command.CommandText = "CREATE TABLE file_entry(id INTEGER, data BLOB);";
        Command.ExecuteNonQuery();

        var entries = new[]
        {
            new FileEntry { Id = 1, Data = new byte[] { 1, 2, 3 } },
            new FileEntry { Id = 2, Data = new byte[] { 10, 20, 30, 40 } },
        };

        using (var appender = Connection.CreateAppender<FileEntry, FileEntryMap>("file_entry"))
        {
            appender.AppendRecords(entries);
        }

        Command.CommandText = "SELECT id, data FROM file_entry ORDER BY id";
        using var reader = Command.ExecuteReader();

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(1);
        ReadBlob(reader, 1).Should().Equal(1, 2, 3);

        reader.Read().Should().BeTrue();
        reader.GetInt32(0).Should().Be(2);
        ReadBlob(reader, 1).Should().Equal(10, 20, 30, 40);

        static byte[] ReadBlob(System.Data.Common.DbDataReader reader, int ordinal)
        {
            using var stream = reader.GetStream(ordinal);
            using var memory = new MemoryStream();
            stream.CopyTo(memory);
            return memory.ToArray();
        }
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

    // One property for every type the mapped appender supports that the other tests do not use.
    public class AllTypes
    {
        public bool Bool { get; set; }
        public sbyte SByte { get; set; }
        public short Short { get; set; }
        public long Long { get; set; }
        public byte Byte { get; set; }
        public ushort UShort { get; set; }
        public uint UInt { get; set; }
        public ulong ULong { get; set; }
        public double Double { get; set; }
        public decimal Decimal { get; set; }
        public DateTimeOffset DateTimeOffset { get; set; }
        public TimeSpan TimeSpan { get; set; }
        public Guid Guid { get; set; }
        public BigInteger BigInteger { get; set; }
        public DateOnly DateOnly { get; set; }
        public TimeOnly TimeOnly { get; set; }
        public int? NullableInt { get; set; }
        public string NullableString { get; set; }
    }

    public class AllTypesMap : DuckDBAppenderMap<AllTypes>
    {
        public AllTypesMap()
        {
            Map(x => x.Bool);
            Map(x => x.SByte);
            Map(x => x.Short);
            Map(x => x.Long);
            Map(x => x.Byte);
            Map(x => x.UShort);
            Map(x => x.UInt);
            Map(x => x.ULong);
            Map(x => x.Double);
            Map(x => x.Decimal);
            Map(x => x.DateTimeOffset);
            Map(x => x.TimeSpan);
            Map(x => x.Guid);
            Map(x => x.BigInteger);
            Map(x => x.DateOnly);
            Map(x => x.TimeOnly);
            Map(x => x.NullableInt);
            Map(x => x.NullableString);
        }
    }

    [Fact]
    public void MappedAppender_SupportsAllMappedTypes()
    {
        Command.CommandText = """
                              CREATE TABLE mapped_all_types(
                                  c_bool BOOLEAN, c_tinyint TINYINT, c_smallint SMALLINT, c_bigint BIGINT,
                                  c_utinyint UTINYINT, c_usmallint USMALLINT, c_uinteger UINTEGER, c_ubigint UBIGINT,
                                  c_double DOUBLE, c_decimal DECIMAL(18, 4), c_timestamptz TIMESTAMPTZ, c_interval INTERVAL,
                                  c_uuid UUID, c_hugeint HUGEINT, c_date DATE, c_time TIME,
                                  c_nullable_int INTEGER, c_nullable_string VARCHAR);
                              """;
        Command.ExecuteNonQuery();

        var record = new AllTypes
        {
            Bool = true,
            SByte = -8,
            Short = -1600,
            Long = -6_400_000_000_000,
            Byte = 200,
            UShort = 60_000,
            UInt = 4_000_000_000,
            ULong = 18_000_000_000_000_000_000,
            Double = 1234.5678,
            Decimal = 12345.6789m,
            DateTimeOffset = new DateTimeOffset(2024, 5, 17, 13, 45, 30, 123, TimeSpan.Zero),
            TimeSpan = new TimeSpan(3, 4, 5, 6, 789),
            Guid = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e"),
            BigInteger = BigInteger.Parse("123456789012345678901234567890"),
            DateOnly = new DateOnly(2024, 5, 17),
            TimeOnly = new TimeOnly(13, 45, 30, 123),
            NullableInt = 42,
            NullableString = "text",
        };

        // The same values again, but with the two nullable properties left null.
        var recordWithNulls = new AllTypes
        {
            Bool = record.Bool, SByte = record.SByte, Short = record.Short, Long = record.Long,
            Byte = record.Byte, UShort = record.UShort, UInt = record.UInt, ULong = record.ULong,
            Double = record.Double, Decimal = record.Decimal, DateTimeOffset = record.DateTimeOffset,
            TimeSpan = record.TimeSpan, Guid = record.Guid, BigInteger = record.BigInteger,
            DateOnly = record.DateOnly, TimeOnly = record.TimeOnly,
        };

        using (var appender = Connection.CreateAppender<AllTypes, AllTypesMap>("mapped_all_types"))
        {
            appender.AppendRecords(new[] { record, recordWithNulls });
        }

        Command.CommandText = "SELECT * FROM mapped_all_types ORDER BY rowid";
        using var reader = Command.ExecuteReader();

        for (var row = 0; row < 2; row++)
        {
            reader.Read().Should().BeTrue();

            reader.GetBoolean(0).Should().Be(record.Bool);
            reader.GetFieldValue<sbyte>(1).Should().Be(record.SByte);
            reader.GetInt16(2).Should().Be(record.Short);
            reader.GetInt64(3).Should().Be(record.Long);
            reader.GetByte(4).Should().Be(record.Byte);
            reader.GetFieldValue<ushort>(5).Should().Be(record.UShort);
            reader.GetFieldValue<uint>(6).Should().Be(record.UInt);
            reader.GetFieldValue<ulong>(7).Should().Be(record.ULong);
            reader.GetDouble(8).Should().Be(record.Double);
            reader.GetDecimal(9).Should().Be(record.Decimal);
            reader.GetFieldValue<DateTimeOffset>(10).UtcDateTime.Should().Be(record.DateTimeOffset.UtcDateTime);
            reader.GetFieldValue<TimeSpan>(11).Should().Be(record.TimeSpan);
            reader.GetGuid(12).Should().Be(record.Guid);
            reader.GetFieldValue<BigInteger>(13).Should().Be(record.BigInteger);
            reader.GetFieldValue<DateOnly>(14).Should().Be(record.DateOnly);
            reader.GetFieldValue<TimeOnly>(15).Should().Be(record.TimeOnly);
        }

        Command.CommandText = "SELECT c_nullable_int, c_nullable_string FROM mapped_all_types ORDER BY rowid";
        using var nullableReader = Command.ExecuteReader();

        nullableReader.Read().Should().BeTrue();
        nullableReader.GetInt32(0).Should().Be(42);
        nullableReader.GetString(1).Should().Be("text");

        nullableReader.Read().Should().BeTrue();
        nullableReader.IsDBNull(0).Should().BeTrue();
        nullableReader.IsDBNull(1).Should().BeTrue();
    }
}
