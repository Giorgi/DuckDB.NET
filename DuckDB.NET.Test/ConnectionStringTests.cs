using System.ComponentModel;

namespace DuckDB.NET.Test;

public class ConnectionStringTests
{
    [Theory]
    [InlineData(DuckDBConnectionStringBuilder.InMemoryConnectionString)]
    [InlineData("DataSource = :memory:")]
    [InlineData("Data Source=:memory:")]
    [InlineData("Data Source = :memory:")]
    [InlineData("DataSource=   :memory:")]
    [InlineData("Data Source=    :memory:")]
    [InlineData("DataSource   =   :memory:")]
    [InlineData("Data Source    =    :memory:")]
    [InlineData("DataSource   =:memory:")]
    [InlineData("Data Source    =:memory:")]
    [InlineData("DataSource=:Memory:")]
    [InlineData("Data Source=:Memory:")]
    [InlineData("datasource=:memory:")]
    [InlineData("data source=:memory:")]
    [InlineData("daTa source=:memory:")]
    public void ExplicitConnectionStringTest(string connectionString)
    {
        using var connection = new DuckDBConnection(connectionString);
        connection.Open();

        connection.State.Should().Be(ConnectionState.Open);
    }

    [Theory]
    [InlineData("DataSource = ")]
    [InlineData("Source=:memory:")]
    [InlineData("Data=:memory:")]
    [InlineData("DataSource = :memory:;Something=else")]
    public void InvalidConnectionStringTests(string connectionString)
    {
        using var connection = new DuckDBConnection(connectionString);
        connection.Invoking(con => con.Open())
            .Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ConnectionStringBuilderDataSourceTest()
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource
        };

        using var connection = new DuckDBConnection(builder.ToString());
        connection.Open();

        connection.State.Should().Be(ConnectionState.Open);
    }

    [Fact]
    public void ConnectionStringPropertiesTest()
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource
        };

        using var connection = new DuckDBConnection(builder.ToString());
        connection.Open();

        connection.Database.Should().Be(DuckDBConnectionStringBuilder.InMemoryDataSource);
        connection.DataSource.Should().Be(DuckDBConnectionStringBuilder.InMemoryDataSource);

        connection.ConnectionString = "";

        connection.Invoking(c => c.Database).Should().Throw<InvalidOperationException>();
        connection.Invoking(c => c.DataSource).Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ConnectionStringBuilderSetPropertiesTest()
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource,
            ["threads"] = 8,
            ["ACCESS_MODE"] = "automatic"
        };

        builder.ConnectionString.Should().Be("DataSource=:memory:;threads=8;ACCESS_MODE=automatic");
    }

    [Fact]
    public void ConnectionStringBuilderSetNotExistingProperty()
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource,
        };

        builder.Invoking(b => b["dummy"] = "prop").Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void ConnectionStringBuilderGetPropertiesTest()
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            ConnectionString = "DataSource = :memory:;Threads = 8;ACCESS_MODE=automatic"
        };

        builder.DataSource.Should().Be(DuckDBConnectionStringBuilder.InMemoryDataSource);
        builder["threads"].Should().Be("8");
        builder["access_mode"].Should().Be("automatic");
    }

    [Fact]
    public void TypedPropertiesWriteTheDuckDBOptionNames()
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource,
            AccessMode = DuckDBAccessMode.ReadWrite,
            Threads = 4,
            MemoryLimit = "512MB",
            PreserveInsertionOrder = false,
            DefaultOrder = DuckDBSortOrder.Descending,
            DefaultNullOrder = DuckDBNullOrder.NullsFirst
        };

        builder.ConnectionString.Should().Be("DataSource=:memory:;access_mode=read_write;threads=4;memory_limit=512MB;" +
                                             "preserve_insertion_order=false;default_order=DESC;default_null_order=NULLS_FIRST");
    }

    [Fact]
    public void TypedPropertiesReadBackFromConnectionString()
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            ConnectionString = "DataSource=:memory:;ACCESS_MODE=READ_ONLY;threads=6;memory_limit=1GB;" +
                               "errors_as_json=true;default_order=ASC;default_null_order=NULLS_LAST"
        };

        builder.AccessMode.Should().Be(DuckDBAccessMode.ReadOnly);
        builder.Threads.Should().Be(6);
        builder.MemoryLimit.Should().Be("1GB");
        builder.ErrorsAsJson.Should().BeTrue();
        builder.DefaultOrder.Should().Be(DuckDBSortOrder.Ascending);
        builder.DefaultNullOrder.Should().Be(DuckDBNullOrder.NullsLast);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(0)]
    public void ThreadsRejectsValuesDuckDBWouldRefuse(int value)
    {
        var builder = new DuckDBConnectionStringBuilder();

        builder.Invoking(b => b.Threads = value).Should().Throw<ArgumentOutOfRangeException>();

        // The same value really is refused by DuckDB, so rejecting it early loses nothing.
        using var connection = new DuckDBConnection($"DataSource=:memory:;threads={value}");
        connection.Invoking(c => c.Open()).Should().Throw<DuckDBException>();
    }

    [Fact]
    public void ExternalThreadsAllowsZeroButNotNegative()
    {
        var builder = new DuckDBConnectionStringBuilder { DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource };

        builder.Invoking(b => b.ExternalThreads = -1).Should().Throw<ArgumentOutOfRangeException>();

        builder.ExternalThreads = 0;

        using var connection = new DuckDBConnection(builder.ConnectionString);
        connection.Open();

        connection.State.Should().Be(ConnectionState.Open);
    }

    [Fact]
    public void ThreadCountGettersStayLenient()
    {
        // A connection string written elsewhere may hold a value the setter would reject. Reading it must not throw;
        // the failure belongs at Open time, where DuckDB reports it.
        var builder = new DuckDBConnectionStringBuilder { ConnectionString = "DataSource=:memory:;threads=-1" };

        builder.Threads.Should().Be(-1);
    }

    [Theory]
    [InlineData("DataSource")]
    [InlineData("Data Source")]
    [InlineData("data source")]
    public void EveryDataSourceSpellingIsStoredUnderOneKeyword(string keyword)
    {
        var builder = new DuckDBConnectionStringBuilder { ConnectionString = $"{keyword}=first.db;threads=4" };

        builder.Keys.Cast<string>().Count(key => key.Replace(" ", "").Equals("DataSource", StringComparison.OrdinalIgnoreCase))
               .Should().Be(1);
        builder.DataSource.Should().Be("first.db");

        // The descriptor must read and write that one key, whatever spelling the connection string arrived in.
        var descriptor = TypeDescriptor.GetProperties(builder).Find(nameof(DuckDBConnectionStringBuilder.DataSource), false)!;

        descriptor.GetValue(builder).Should().Be("first.db");
        descriptor.ShouldSerializeValue(builder).Should().BeTrue();

        descriptor.SetValue(builder, "second.db");

        builder.DataSource.Should().Be("second.db");
        builder.ConnectionString.Should().NotContain("first.db");
    }

    [Theory]
    [InlineData("DataSource")]
    [InlineData("Data Source")]
    [InlineData("data source")]
    public void EveryDataSourceSpellingWorksAcrossTheDictionaryMembers(string keyword)
    {
        // Values are stored under one keyword, so every read path has to translate the alias too. A setter that
        // accepts a spelling its own getter rejects would be a bug on its face, and Remove would silently no-op.
        var builder = new DuckDBConnectionStringBuilder { ConnectionString = "Data Source=first.db;threads=4" };

        builder[keyword].Should().Be("first.db");
        builder.ContainsKey(keyword).Should().BeTrue();
        builder.TryGetValue(keyword, out var value).Should().BeTrue();
        value.Should().Be("first.db");
        builder.ShouldSerialize(keyword).Should().BeTrue();

        builder[keyword] = "second.db";
        builder.DataSource.Should().Be("second.db");

        builder.Remove(keyword).Should().BeTrue();
        builder.ContainsKey(keyword).Should().BeFalse();
        builder.ConnectionString.Should().Be("threads=4");
    }

    [Fact]
    public void UnsetTypedPropertiesAreNullOrEmpty()
    {
        var builder = new DuckDBConnectionStringBuilder { DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource };

        builder.AccessMode.Should().BeNull();
        builder.Threads.Should().BeNull();
        builder.EnableExternalAccess.Should().BeNull();
        builder.DefaultOrder.Should().BeNull();
        builder.MemoryLimit.Should().BeEmpty();
        builder.TempDirectory.Should().BeEmpty();
    }

    [Fact]
    public void SettingTypedPropertyToNullRemovesTheKey()
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource,
            Threads = 4,
            AccessMode = DuckDBAccessMode.ReadOnly,
            EnableExternalAccess = false
        };

        builder.Threads = null;
        builder.AccessMode = null;
        builder.EnableExternalAccess = null;

        builder.ContainsKey("threads").Should().BeFalse();
        builder.ContainsKey("access_mode").Should().BeFalse();
        builder.ContainsKey("enable_external_access").Should().BeFalse();
        builder.ConnectionString.Should().Be("DataSource=:memory:");
    }

    [Theory]
    [InlineData("true", true)]
    [InlineData("True", true)]
    [InlineData("TRUE", true)]
    [InlineData("1", true)]
    [InlineData("yes", true)]
    [InlineData("t", true)]
    [InlineData("y", true)]
    [InlineData("false", false)]
    [InlineData("0", false)]
    [InlineData("no", false)]
    [InlineData("f", false)]
    [InlineData("n", false)]
    public void BooleanPropertiesAcceptTheFormsDuckDBAccepts(string value, bool expected)
    {
        // Every spelling here opens a connection successfully, so reading it back must not throw.
        using (var connection = new DuckDBConnection($"DataSource=:memory:;enable_external_access={value}"))
        {
            connection.Open();
        }

        var builder = new DuckDBConnectionStringBuilder { ConnectionString = $"DataSource=:memory:;enable_external_access={value}" };

        builder.EnableExternalAccess.Should().Be(expected);
    }

    [Theory]
    [InlineData("NULLS_FIRST", DuckDBNullOrder.NullsFirst)]
    [InlineData("nulls first", DuckDBNullOrder.NullsFirst)]
    [InlineData("null first", DuckDBNullOrder.NullsFirst)]
    [InlineData("first", DuckDBNullOrder.NullsFirst)]
    [InlineData("NULLS_LAST", DuckDBNullOrder.NullsLast)]
    [InlineData("last", DuckDBNullOrder.NullsLast)]
    [InlineData("sqlite", DuckDBNullOrder.NullsFirstOnAscLastOnDesc)]
    [InlineData("mysql", DuckDBNullOrder.NullsFirstOnAscLastOnDesc)]
    [InlineData("NULLS_FIRST_ON_ASC_LAST_ON_DESC", DuckDBNullOrder.NullsFirstOnAscLastOnDesc)]
    [InlineData("postgres", DuckDBNullOrder.NullsLastOnAscFirstOnDesc)]
    [InlineData("NULLS_LAST_ON_ASC_FIRST_ON_DESC", DuckDBNullOrder.NullsLastOnAscFirstOnDesc)]
    public void DefaultNullOrderAcceptsEverySpellingDuckDBAccepts(string value, DuckDBNullOrder expected)
    {
        using (var connection = new DuckDBConnection($"DataSource=:memory:;default_null_order={value}"))
        {
            connection.Open();
        }

        var builder = new DuckDBConnectionStringBuilder { ConnectionString = $"DataSource=:memory:;default_null_order={value}" };

        builder.DefaultNullOrder.Should().Be(expected);
    }

    [Theory]
    [InlineData(DuckDBNullOrder.NullsFirst, "NULLS_FIRST")]
    [InlineData(DuckDBNullOrder.NullsLast, "NULLS_LAST")]
    [InlineData(DuckDBNullOrder.NullsFirstOnAscLastOnDesc, "NULLS_FIRST_ON_ASC_LAST_ON_DESC")]
    [InlineData(DuckDBNullOrder.NullsLastOnAscFirstOnDesc, "NULLS_LAST_ON_ASC_FIRST_ON_DESC")]
    public void DefaultNullOrderWritesValuesDuckDBAccepts(DuckDBNullOrder order, string expected)
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource,
            DefaultNullOrder = order
        };

        builder["default_null_order"].Should().Be(expected);

        using var connection = new DuckDBConnection(builder.ConnectionString);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT current_setting('default_null_order')";
        command.ExecuteScalar()!.ToString().Should().Be(expected);
    }

    [Fact]
    public void TypedPropertiesThrowOnMalformedValues()
    {
        var builder = new DuckDBConnectionStringBuilder { ConnectionString = "DataSource=:memory:;threads=lots;access_mode=sideways" };

        builder.Invoking(b => b.Threads).Should().Throw<InvalidOperationException>();
        builder.Invoking(b => b.AccessMode).Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public void TypedPropertiesProduceAConnectionStringDuckDBAccepts()
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource,
            AccessMode = DuckDBAccessMode.ReadWrite,
            Threads = 2,
            ExternalThreads = 1,
            MemoryLimit = "256MB",
            CheckpointThreshold = "16MB",
            EnableExternalAccess = true,
            AllowUnsignedExtensions = false,
            AllowCommunityExtensions = false,
            AllowPersistentSecrets = false,
            AutoloadKnownExtensions = false,
            AutoinstallKnownExtensions = false,
            LockConfiguration = false,
            PreserveInsertionOrder = false,
            DefaultOrder = DuckDBSortOrder.Descending,
            DefaultNullOrder = DuckDBNullOrder.NullsFirst,
            ErrorsAsJson = false,
            CustomUserAgent = "DuckDB.NET.Test"
        };

        using var connection = new DuckDBConnection(builder.ConnectionString);
        connection.Open();

        connection.State.Should().Be(ConnectionState.Open);

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT current_setting('threads'), current_setting('memory_limit'), current_setting('default_null_order')";

        using var reader = command.ExecuteReader();
        reader.Read().Should().BeTrue();
        reader.GetInt64(0).Should().Be(2);
        reader.GetString(1).Should().Be("244.1 MiB");
        reader.GetString(2).Should().Be("NULLS_FIRST");
    }

    [Fact]
    public void TypedPropertiesAreDiscoverableThroughTypeDescriptor()
    {
        var properties = TypeDescriptor.GetProperties(new DuckDBConnectionStringBuilder());

        var configurationProperties = properties.Cast<PropertyDescriptor>()
                                                .Where(property => !string.IsNullOrEmpty(property.Description))
                                                .ToList();

        configurationProperties.Should().HaveCountGreaterThan(20);
        configurationProperties.Should().OnlyContain(property => property.Category != "Misc");

        var threads = properties.Find(nameof(DuckDBConnectionStringBuilder.Threads), false);

        threads.Should().NotBeNull();
        threads!.PropertyType.Should().Be(typeof(int?));
        threads.DisplayName.Should().Be("threads");
        threads.Category.Should().Be("Resources");
        threads.Description.Should().NotBeEmpty();
    }

    [Theory]
    [InlineData("DataSource")]
    [InlineData("Data Source")]
    [InlineData("data source")]
    public void SetOptionsAreNotDescribedTwice(string dataSourceKeyword)
    {
        // DbConnectionStringBuilder synthesizes an undescribed descriptor for every keyword present in the connection
        // string, keyed by DisplayName. Any accepted spelling of a keyword that a typed property also covers must not
        // produce a second entry - comparison ignores spaces so that "Data Source" and "DataSource" collide here.
        var builder = new DuckDBConnectionStringBuilder
        {
            ConnectionString = $"{dataSourceKeyword}=:memory:;threads=4;access_mode=read_only;memory_limit=1GB"
        };

        var duplicates = TypeDescriptor.GetProperties(builder)
                                       .Cast<PropertyDescriptor>()
                                       .GroupBy(property => property.Name.Replace(" ", ""), StringComparer.OrdinalIgnoreCase)
                                       .Where(group => group.Count() > 1)
                                       .Select(group => group.Key);

        duplicates.Should().BeEmpty();
    }

    [Fact]
    public void EveryConfigurationOptionIsDiscoverableThroughTypeDescriptor()
    {
        var properties = TypeDescriptor.GetProperties(new DuckDBConnectionStringBuilder());

        // duckdb_get_config_flag reports the options of the native library in use; all of them should be reachable
        // apart from the handful that cannot be applied when the database is opened.
        var configCount = NativeMethods.Configuration.DuckDBConfigCount();

        configCount.Should().BeGreaterThan(100);
        properties.Count.Should().BeGreaterThan(configCount - 20);

        var option = properties.Find("zstd_min_string_length", false);

        option.Should().NotBeNull();
        option!.Description.Should().NotBeEmpty();
        option.PropertyType.Should().Be(typeof(ulong));
    }

    [Theory]
    [InlineData("arrow_output_list_view", "Arrow")]
    [InlineData("profiling_mode", "Profiling")]
    [InlineData("enable_http_logging", "Logging")]      // logging wins over the http segment
    [InlineData("logging_level", "Logging")]
    [InlineData("http_proxy_username", "Network")]      // network wins over the username segment
    [InlineData("default_secret_storage", "Security")]  // security wins over the storage segment
    [InlineData("autoinstall_extension_repository", "Extensions")]
    [InlineData("wal_autocheckpoint", "Storage")]
    [InlineData("block_allocator_memory", "Resources")] // allocator wins over the block segment
    [InlineData("streaming_buffer_size", "Resources")]
    [InlineData("nested_loop_join_threshold", "Optimizer")]
    [InlineData("prefer_range_joins", "Optimizer")]     // plural segment
    [InlineData("zstd_min_string_length", "Storage")]
    [InlineData("disabled_filesystems", "Security")]
    [InlineData("integer_division", "Query")]           // dialect options are listed, not matched
    [InlineData("null_order", "Query")]
    [InlineData("search_path", "Query")]
    [InlineData("home_directory", "General")]
    public void ConfigurationOptionsAreGroupedByName(string option, string expectedCategory)
    {
        var properties = TypeDescriptor.GetProperties(new DuckDBConnectionStringBuilder());

        properties.Find(option, false)!.Category.Should().Be(expectedCategory);
    }

    [Fact]
    public void OptionsAreNotGroupedBySubstringAccident()
    {
        var properties = TypeDescriptor.GetProperties(new DuckDBConnectionStringBuilder());

        // "catalog_error_max_schemas" contains "log", but not as a segment.
        properties.Find("catalog_error_max_schemas", false)!.Category.Should().NotBe("Logging");
    }

    [Fact]
    public void ConfigurationOptionDescriptorsCarryTypesFromDuckDBSettings()
    {
        var properties = TypeDescriptor.GetProperties(new DuckDBConnectionStringBuilder());

        properties.Find("arrow_large_buffer_size", false)!.PropertyType.Should().Be(typeof(bool));
        properties.Find("index_scan_percentage", false)!.PropertyType.Should().Be(typeof(double));
        properties.Find("geometry_minimum_shredding_size", false)!.PropertyType.Should().Be(typeof(long));
        properties.Find("default_secret_storage", false)!.PropertyType.Should().Be(typeof(string));

        // Options of an extension that is not loaded have no type metadata and no description; DuckDB reports the
        // owning extension name instead, which is used as the category.
        var azure = properties.Find("azure_endpoint", false);

        azure.Should().NotBeNull();
        azure!.PropertyType.Should().Be(typeof(string));
        azure.Category.Should().Be("azure");
    }

    [Fact]
    public void ConfigurationOptionDescriptorsReadAndWriteTheConnectionString()
    {
        var builder = new DuckDBConnectionStringBuilder { DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource };
        var option = TypeDescriptor.GetProperties(builder).Find("zstd_min_string_length", false)!;

        option.ShouldSerializeValue(builder).Should().BeFalse();
        option.GetValue(builder).Should().BeNull();

        option.SetValue(builder, 8192UL);

        builder.ConnectionString.Should().Be("DataSource=:memory:;zstd_min_string_length=8192");
        option.GetValue(builder).Should().Be(8192UL);
        option.ShouldSerializeValue(builder).Should().BeTrue();

        option.ResetValue(builder);

        builder.ConnectionString.Should().Be("DataSource=:memory:");
    }

    [Fact]
    public void BooleanOptionDescriptorsWriteValuesDuckDBAccepts()
    {
        var builder = new DuckDBConnectionStringBuilder { DataSource = DuckDBConnectionStringBuilder.InMemoryDataSource };
        var option = TypeDescriptor.GetProperties(builder).Find("arrow_large_buffer_size", false)!;

        option.SetValue(builder, true);

        builder.ConnectionString.Should().Be("DataSource=:memory:;arrow_large_buffer_size=true");

        using var connection = new DuckDBConnection(builder.ConnectionString);
        connection.Open();

        connection.State.Should().Be(ConnectionState.Open);
    }

    [Fact]
    public void OptionsThatCannotBeAppliedAtOpenAreNotOffered()
    {
        var properties = TypeDescriptor.GetProperties(new DuckDBConnectionStringBuilder());

        // duckdb_set_config rejects every string form of a VARCHAR[] value.
        properties.Find("allowed_directories", false).Should().BeNull();
        properties.Find("allowed_paths", false).Should().BeNull();
        properties.Find("allowed_configs", false).Should().BeNull();
        properties.Find("extension_directories", false).Should().BeNull();
    }

    [Fact]
    public void DebugOptionsAreHiddenButReachable()
    {
        var properties = TypeDescriptor.GetProperties(new DuckDBConnectionStringBuilder());
        var debugOption = properties.Find("debug_window_mode", false);

        debugOption.Should().NotBeNull();
        debugOption!.IsBrowsable.Should().BeFalse();
        debugOption.Category.Should().Be("Debug");
    }

    [Fact]
    public void TypedPropertiesAreNotReplacedByGeneratedDescriptors()
    {
        var properties = TypeDescriptor.GetProperties(new DuckDBConnectionStringBuilder());

        // Generated descriptors are named after the keyword, typed properties after the CLR member. Finding no
        // descriptor named "threads" is what proves the typed Threads property was excluded from generation.
        properties.Find("threads", false).Should().BeNull();

        var threads = properties.Find(nameof(DuckDBConnectionStringBuilder.Threads), false);

        threads.Should().NotBeNull();
        threads!.PropertyType.Should().Be(typeof(int?));
        threads.Category.Should().Be("Resources");
        threads.DisplayName.Should().Be("threads");
    }
}