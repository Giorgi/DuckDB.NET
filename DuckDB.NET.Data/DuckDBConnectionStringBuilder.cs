using DuckDB.NET.Data.Connection;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace DuckDB.NET.Data;

public class DuckDBConnectionStringBuilder : DbConnectionStringBuilder
{
    private static readonly HashSet<string> DataSourceKeys = new(StringComparer.OrdinalIgnoreCase) { "Data Source", "DataSource" };
    private static readonly HashSet<string> ConfigurationOptions = new(StringComparer.OrdinalIgnoreCase);

    public const string InMemoryDataSource = ":memory:";
    public const string InMemoryConnectionString = "DataSource=:memory:";

    public const string InMemorySharedDataSource = ":memory:?cache=shared";
    public const string InMemorySharedConnectionString = "DataSource=:memory:?cache=shared";

    private const string DataSourceKey = "DataSource";
    private const string DuckDBApiConfigKey = "duckdb_api";

    private const string AccessModeKey = "access_mode";
    private const string ThreadsKey = "threads";
    private const string ExternalThreadsKey = "external_threads";
    private const string MemoryLimitKey = "memory_limit";
    private const string TempDirectoryKey = "temp_directory";
    private const string MaxTempDirectorySizeKey = "max_temp_directory_size";
    private const string CheckpointThresholdKey = "checkpoint_threshold";
    private const string StorageCompatibilityVersionKey = "storage_compatibility_version";
    private const string EnableExternalAccessKey = "enable_external_access";
    private const string AllowUnsignedExtensionsKey = "allow_unsigned_extensions";
    private const string AllowCommunityExtensionsKey = "allow_community_extensions";
    private const string AllowPersistentSecretsKey = "allow_persistent_secrets";
    private const string SecretDirectoryKey = "secret_directory";
    private const string LockConfigurationKey = "lock_configuration";
    private const string AutoloadKnownExtensionsKey = "autoload_known_extensions";
    private const string AutoinstallKnownExtensionsKey = "autoinstall_known_extensions";
    private const string ExtensionDirectoryKey = "extension_directory";
    private const string CustomExtensionRepositoryKey = "custom_extension_repository";
    private const string DefaultOrderKey = "default_order";
    private const string DefaultNullOrderKey = "default_null_order";
    private const string DefaultCollationKey = "default_collation";
    private const string PreserveInsertionOrderKey = "preserve_insertion_order";
    private const string CustomUserAgentKey = "custom_user_agent";
    private const string ErrorsAsJsonKey = "errors_as_json";

    private static readonly string DuckDBApi;

    /// <summary>
    /// Keywords already exposed as typed properties. A dynamic descriptor registered under one of these would silently
    /// replace the typed property, since both are keyed by the same display name.
    /// </summary>
    private static readonly HashSet<string> TypedPropertyKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        DataSourceKey, "Data Source", AccessModeKey, ThreadsKey, ExternalThreadsKey, MemoryLimitKey, TempDirectoryKey,
        MaxTempDirectorySizeKey, CheckpointThresholdKey, StorageCompatibilityVersionKey, EnableExternalAccessKey,
        AllowUnsignedExtensionsKey, AllowCommunityExtensionsKey, AllowPersistentSecretsKey, SecretDirectoryKey,
        LockConfigurationKey, AutoloadKnownExtensionsKey, AutoinstallKnownExtensionsKey, ExtensionDirectoryKey,
        CustomExtensionRepositoryKey, DefaultOrderKey, DefaultNullOrderKey, DefaultCollationKey,
        PreserveInsertionOrderKey, CustomUserAgentKey, ErrorsAsJsonKey
    };


    /// <summary>
    /// Options that change how SQL is parsed or evaluated. These share no common word, so unlike every other grouping
    /// rule they have to be listed. The set is small and slow moving; an option added by a later DuckDB version simply
    /// falls into the general group until it is added here.
    /// </summary>
    private static readonly HashSet<string> QueryOptions = new(StringComparer.OrdinalIgnoreCase)
    {
        "integer_division", "ieee_floating_point_ops", "old_implicit_casting", "disable_timestamptz_casts",
        "order_by_non_integer_literal", "preserve_identifier_case", "scalar_subquery_error_on_multiple_rows",
        "null_order", "lambda_syntax", "deprecated_using_key_syntax", "explain_output", "max_expression_depth",
        "schema", "search_path"
    };

    private static readonly Lazy<PropertyDescriptor[]> ConfigurationDescriptors = new(BuildConfigurationDescriptors);

    static DuckDBConnectionStringBuilder()
    {
        var configCount = NativeMethods.Configuration.DuckDBConfigCount();

        for (var index = 0; index < configCount; index++)
        {
            NativeMethods.Configuration.DuckDBGetConfigFlag(index, out var name, out _);
            ConfigurationOptions.Add(name);
        }

#if CI
        DuckDBApi = $"DuckDB.NET/{GitVersionInformation.FullSemVer}"; 
#else
        DuckDBApi = $"DuckDB.NET";
#endif
    }

    internal static DuckDBConnectionString Parse(string connectionString)
    {
        var builder = new DuckDBConnectionStringBuilder
        {
            ConnectionString = connectionString
        };

        if (!builder.ContainsKey(DuckDBApiConfigKey))
        {
            builder[DuckDBApiConfigKey] = DuckDBApi;
        }
        var dataSource = builder.DataSource;

        var configurations = new Dictionary<string, string>();

        foreach (KeyValuePair<string, object> pair in builder)
        {
            if (DataSourceKeys.Contains(pair.Key))
            {
                continue;
            }

            configurations.Add(pair.Key, pair.Value.ToString()!);
        }

        if (string.IsNullOrEmpty(dataSource))
        {
            throw new InvalidOperationException($"Connection string '{connectionString}' is not valid, missing data source information.");
        }

        var inMemory = dataSource.Equals(InMemoryDataSource, StringComparison.OrdinalIgnoreCase);

        var isShared = dataSource.Equals(InMemorySharedDataSource, StringComparison.OrdinalIgnoreCase);
        if (isShared)
        {
            inMemory = true;
        }

        return new DuckDBConnectionString(dataSource, inMemory, isShared, configurations);
    }

    [AllowNull]
    public override object this[string keyword]
    {
        get => base[Normalize(keyword)];
        set
        {
            // Every accepted data source spelling is stored under one keyword. Keeping two spellings for one option
            // lets a connection string end up holding both, where the stale one silently wins.
            if (DataSourceKeys.Contains(keyword))
            {
                base[DataSourceKey] = value;
            }
            else if (ConfigurationOptions.Contains(keyword))
            {
                base[keyword] = value;
            }
            else
            {
                throw new InvalidOperationException($"Unrecognized connection string property '{keyword}'");
            }
        }
    }

    // DisplayName must match the keyword this property writes. DbConnectionStringBuilder keys its
    // descriptor table by DisplayName and also synthesizes a descriptor for every keyword present in
    // the connection string, so a mismatch surfaces the option twice in a property grid.
    [Category("Data Source")]
    [DisplayName(DataSourceKey)]
    [Description("Path to the database file, ':memory:' for an in-memory database, or an 'md:' MotherDuck target.")]
    public string DataSource
    {
        // The indexer normalises every accepted spelling to DataSourceKey, so only that one can be present.
        get => GetString(DataSourceKey);
        set => this[DataSourceKey] = value;
    }

    /// <summary>
    /// Maps any accepted spelling of a keyword onto the one it is stored under. Values are only ever written under the
    /// canonical keyword, so every read path has to translate or an alias would appear to be absent.
    /// </summary>
    private static string Normalize(string keyword) => DataSourceKeys.Contains(keyword) ? DataSourceKey : keyword;

    /// <inheritdoc />
    public override bool ContainsKey(string keyword) => base.ContainsKey(Normalize(keyword));

    /// <inheritdoc />
    public override bool TryGetValue(string keyword, out object value) => base.TryGetValue(Normalize(keyword), out value);

    /// <inheritdoc />
    public override bool Remove(string keyword) => base.Remove(Normalize(keyword));

    /// <inheritdoc />
    public override bool ShouldSerialize(string keyword) => base.ShouldSerialize(Normalize(keyword));

    /// <summary>
    /// Access mode of the database. <see langword="null"/> leaves the option unset, so DuckDB's own default applies.
    /// </summary>
    [Category("Data Source")]
    [DisplayName("access_mode")]
    [Description("Access mode of the database (Automatic, ReadOnly or ReadWrite).")]
    [DefaultValue(null)]
    public DuckDBAccessMode? AccessMode
    {
        get
        {
            var value = GetString(AccessModeKey);

            return value switch
            {
                "" => null,
                _ when value.Equals("automatic", StringComparison.OrdinalIgnoreCase) => DuckDBAccessMode.Automatic,
                _ when value.Equals("read_only", StringComparison.OrdinalIgnoreCase) => DuckDBAccessMode.ReadOnly,
                _ when value.Equals("read_write", StringComparison.OrdinalIgnoreCase) => DuckDBAccessMode.ReadWrite,
                _ => throw new InvalidOperationException($"Connection string property '{AccessModeKey}' has value '{value}' which is not a valid access mode.")
            };
        }
        set => this[AccessModeKey] = value switch
        {
            null => null,
            DuckDBAccessMode.Automatic => "automatic",
            DuckDBAccessMode.ReadOnly => "read_only",
            DuckDBAccessMode.ReadWrite => "read_write",
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unknown access mode.")
        };
    }

    /// <summary>
    /// The total number of threads used by the system. DuckDB rejects zero and negative values.
    /// </summary>
    [Category("Resources")]
    [DisplayName("threads")]
    [Description("The number of total threads used by the system. Defaults to the number of available cores.")]
    [DefaultValue(null)]
    public int? Threads
    {
        get => GetInt32(ThreadsKey);
        set
        {
            // DuckDB rejects 0 as well as negatives, so "not negative" would still let a value through that fails
            // at Open time. The getter stays lenient: a connection string written elsewhere may hold anything.
            if (value.HasValue && value.Value < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "Threads must be at least 1.");
            }

            SetInt32(ThreadsKey, value);
        }
    }

    /// <summary>
    /// The number of external threads that work on DuckDB tasks.
    /// </summary>
    [Category("Resources")]
    [DisplayName("external_threads")]
    [Description("The number of external threads that work on DuckDB tasks.")]
    [DefaultValue(null)]
    public int? ExternalThreads
    {
        get => GetInt32(ExternalThreadsKey);
        set
        {
            // Unlike Threads, DuckDB accepts 0 here; only negatives are invalid.
            if (value.HasValue && value.Value < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "ExternalThreads cannot be negative.");
            }

            SetInt32(ExternalThreadsKey, value);
        }
    }

    /// <summary>
    /// The maximum memory the system may use, for example <c>1GB</c>.
    /// </summary>
    [Category("Resources")]
    [DisplayName("memory_limit")]
    [Description("The maximum memory of the system (e.g. 1GB). Defaults to 80% of available memory.")]
    [DefaultValue("")]
    public string MemoryLimit
    {
        get => GetString(MemoryLimitKey);
        set => this[MemoryLimitKey] = value;
    }

    /// <summary>
    /// The directory to which temporary files are written when data does not fit in memory.
    /// </summary>
    [Category("Resources")]
    [DisplayName("temp_directory")]
    [Description("Set the directory to which to write temp files.")]
    [DefaultValue("")]
    public string TempDirectory
    {
        get => GetString(TempDirectoryKey);
        set => this[TempDirectoryKey] = value;
    }

    /// <summary>
    /// The maximum amount of data stored inside <see cref="TempDirectory"/>, for example <c>1GB</c>.
    /// </summary>
    [Category("Resources")]
    [DisplayName("max_temp_directory_size")]
    [Description("The maximum amount of data stored inside the temp directory (e.g. 1GB).")]
    [DefaultValue("")]
    public string MaxTempDirectorySize
    {
        get => GetString(MaxTempDirectorySizeKey);
        set => this[MaxTempDirectorySizeKey] = value;
    }

    /// <summary>
    /// The write-ahead log size threshold at which a checkpoint is triggered automatically, for example <c>1GB</c>.
    /// </summary>
    [Category("Storage")]
    [DisplayName("checkpoint_threshold")]
    [Description("The WAL size threshold at which to automatically trigger a checkpoint (e.g. 1GB).")]
    [DefaultValue("")]
    public string CheckpointThreshold
    {
        get => GetString(CheckpointThresholdKey);
        set => this[CheckpointThresholdKey] = value;
    }

    /// <summary>
    /// Serialize on checkpoint with storage compatibility for the given DuckDB version, for example <c>v0.10.2</c>.
    /// </summary>
    [Category("Storage")]
    [DisplayName("storage_compatibility_version")]
    [Description("Serialize on checkpoint with compatibility for a given DuckDB version (e.g. v0.10.2).")]
    [DefaultValue("")]
    public string StorageCompatibilityVersion
    {
        get => GetString(StorageCompatibilityVersionKey);
        set => this[StorageCompatibilityVersionKey] = value;
    }

    /// <summary>
    /// Whether the database may access external state such as files, extensions and remote endpoints.
    /// </summary>
    [Category("Security")]
    [DisplayName("enable_external_access")]
    [Description("Allow the database to access external state (loading/installing extensions, COPY TO/FROM, file readers).")]
    [DefaultValue(null)]
    public bool? EnableExternalAccess
    {
        get => GetBoolean(EnableExternalAccessKey);
        set => SetBoolean(EnableExternalAccessKey, value);
    }

    /// <summary>
    /// Whether extensions with an invalid or missing signature may be loaded.
    /// </summary>
    [Category("Security")]
    [DisplayName("allow_unsigned_extensions")]
    [Description("Allow to load extensions with invalid or missing signatures.")]
    [DefaultValue(null)]
    public bool? AllowUnsignedExtensions
    {
        get => GetBoolean(AllowUnsignedExtensionsKey);
        set => SetBoolean(AllowUnsignedExtensionsKey, value);
    }

    /// <summary>
    /// Whether community-built extensions may be loaded.
    /// </summary>
    [Category("Security")]
    [DisplayName("allow_community_extensions")]
    [Description("Allow to load community built extensions.")]
    [DefaultValue(null)]
    public bool? AllowCommunityExtensions
    {
        get => GetBoolean(AllowCommunityExtensionsKey);
        set => SetBoolean(AllowCommunityExtensionsKey, value);
    }

    /// <summary>
    /// Whether persistent secrets, which are stored on disk and reloaded on restart, may be created.
    /// </summary>
    [Category("Security")]
    [DisplayName("allow_persistent_secrets")]
    [Description("Allow the creation of persistent secrets, that are stored and loaded on restarts.")]
    [DefaultValue(null)]
    public bool? AllowPersistentSecrets
    {
        get => GetBoolean(AllowPersistentSecretsKey);
        set => SetBoolean(AllowPersistentSecretsKey, value);
    }

    /// <summary>
    /// The directory to which persistent secrets are stored.
    /// </summary>
    [Category("Security")]
    [DisplayName("secret_directory")]
    [Description("Set the directory to which persistent secrets are stored.")]
    [DefaultValue("")]
    public string SecretDirectory
    {
        get => GetString(SecretDirectoryKey);
        set => this[SecretDirectoryKey] = value;
    }

    /// <summary>
    /// Whether configuration options can still be altered after the database has been opened.
    /// </summary>
    [Category("Security")]
    [DisplayName("lock_configuration")]
    [Description("Whether or not configurations can be altered.")]
    [DefaultValue(null)]
    public bool? LockConfiguration
    {
        get => GetBoolean(LockConfigurationKey);
        set => SetBoolean(LockConfigurationKey, value);
    }

    /// <summary>
    /// Whether known extensions may be loaded automatically when a query depends on them.
    /// </summary>
    [Category("Extensions")]
    [DisplayName("autoload_known_extensions")]
    [Description("Whether known extensions are allowed to be automatically loaded when a query depends on them.")]
    [DefaultValue(null)]
    public bool? AutoloadKnownExtensions
    {
        get => GetBoolean(AutoloadKnownExtensionsKey);
        set => SetBoolean(AutoloadKnownExtensionsKey, value);
    }

    /// <summary>
    /// Whether known extensions may be installed automatically when a query depends on them.
    /// </summary>
    [Category("Extensions")]
    [DisplayName("autoinstall_known_extensions")]
    [Description("Whether known extensions are allowed to be automatically installed when a query depends on them.")]
    [DefaultValue(null)]
    public bool? AutoinstallKnownExtensions
    {
        get => GetBoolean(AutoinstallKnownExtensionsKey);
        set => SetBoolean(AutoinstallKnownExtensionsKey, value);
    }

    /// <summary>
    /// The directory in which extensions are stored.
    /// </summary>
    [Category("Extensions")]
    [DisplayName("extension_directory")]
    [Description("Set the directory to store extensions in.")]
    [DefaultValue("")]
    public string ExtensionDirectory
    {
        get => GetString(ExtensionDirectoryKey);
        set => this[ExtensionDirectoryKey] = value;
    }

    /// <summary>
    /// Overrides the endpoint used for remote extension installation.
    /// </summary>
    [Category("Extensions")]
    [DisplayName("custom_extension_repository")]
    [Description("Overrides the custom endpoint for remote extension installation.")]
    [DefaultValue("")]
    public string CustomExtensionRepository
    {
        get => GetString(CustomExtensionRepositoryKey);
        set => this[CustomExtensionRepositoryKey] = value;
    }

    /// <summary>
    /// The sort order used when an <c>ORDER BY</c> clause does not specify one.
    /// </summary>
    [Category("Query")]
    [DisplayName("default_order")]
    [Description("The order type used when none is specified (Ascending or Descending).")]
    [DefaultValue(null)]
    public DuckDBSortOrder? DefaultOrder
    {
        get
        {
            var value = GetString(DefaultOrderKey);

            return value switch
            {
                "" => null,
                _ when value.Equals("asc", StringComparison.OrdinalIgnoreCase) => DuckDBSortOrder.Ascending,
                _ when value.Equals("ascending", StringComparison.OrdinalIgnoreCase) => DuckDBSortOrder.Ascending,
                _ when value.Equals("desc", StringComparison.OrdinalIgnoreCase) => DuckDBSortOrder.Descending,
                _ when value.Equals("descending", StringComparison.OrdinalIgnoreCase) => DuckDBSortOrder.Descending,
                _ => throw new InvalidOperationException($"Connection string property '{DefaultOrderKey}' has value '{value}' which is not a valid sort order.")
            };
        }
        set => this[DefaultOrderKey] = value switch
        {
            null => null,
            DuckDBSortOrder.Ascending => "ASC",
            DuckDBSortOrder.Descending => "DESC",
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unknown sort order.")
        };
    }

    /// <summary>
    /// The NULL ordering used when an <c>ORDER BY</c> clause does not specify one.
    /// </summary>
    [Category("Query")]
    [DisplayName("default_null_order")]
    [Description("NULL ordering used when none is specified (NullsFirst or NullsLast).")]
    [DefaultValue(null)]
    public DuckDBNullOrder? DefaultNullOrder
    {
        get
        {
            var value = GetString(DefaultNullOrderKey);

            // Every spelling DuckDB itself accepts, from DefaultNullOrderSetting::OnSet.
            return value.ToLowerInvariant() switch
            {
                "" => null,
                "nulls_first" or "nulls first" or "null first" or "first" => DuckDBNullOrder.NullsFirst,
                "nulls_last" or "nulls last" or "null last" or "last" => DuckDBNullOrder.NullsLast,
                "nulls_first_on_asc_last_on_desc" or "sqlite" or "mysql" => DuckDBNullOrder.NullsFirstOnAscLastOnDesc,
                "nulls_last_on_asc_first_on_desc" or "postgres" => DuckDBNullOrder.NullsLastOnAscFirstOnDesc,
                _ => throw new InvalidOperationException($"Connection string property '{DefaultNullOrderKey}' has value '{value}' which is not a valid NULL order.")
            };
        }
        set => this[DefaultNullOrderKey] = value switch
        {
            null => null,
            DuckDBNullOrder.NullsFirst => "NULLS_FIRST",
            DuckDBNullOrder.NullsLast => "NULLS_LAST",
            DuckDBNullOrder.NullsFirstOnAscLastOnDesc => "NULLS_FIRST_ON_ASC_LAST_ON_DESC",
            DuckDBNullOrder.NullsLastOnAscFirstOnDesc => "NULLS_LAST_ON_ASC_FIRST_ON_DESC",
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unknown NULL order.")
        };
    }

    /// <summary>
    /// The collation used when none is specified.
    /// </summary>
    [Category("Query")]
    [DisplayName("default_collation")]
    [Description("The collation setting used when none is specified.")]
    [DefaultValue("")]
    public string DefaultCollation
    {
        get => GetString(DefaultCollationKey);
        set => this[DefaultCollationKey] = value;
    }

    /// <summary>
    /// Whether insertion order is preserved. Setting this to <see langword="false"/> lets DuckDB re-order
    /// results that have no <c>ORDER BY</c> clause, which can reduce memory usage.
    /// </summary>
    [Category("Query")]
    [DisplayName("preserve_insertion_order")]
    [Description("Whether or not to preserve insertion order. If false the system may re-order results that do not contain ORDER BY.")]
    [DefaultValue(null)]
    public bool? PreserveInsertionOrder
    {
        get => GetBoolean(PreserveInsertionOrderKey);
        set => SetBoolean(PreserveInsertionOrderKey, value);
    }

    /// <summary>
    /// Metadata identifying the calling application, reported to DuckDB alongside the driver's own identifier.
    /// </summary>
    [Category("Client")]
    [DisplayName("custom_user_agent")]
    [Description("Metadata from DuckDB callers, appended to the user agent reported by the driver.")]
    [DefaultValue("")]
    public string CustomUserAgent
    {
        get => GetString(CustomUserAgentKey);
        set => this[CustomUserAgentKey] = value;
    }

    /// <summary>
    /// Whether error messages are produced as structured JSON instead of a raw string.
    /// </summary>
    [Category("Client")]
    [DisplayName("errors_as_json")]
    [Description("Output error messages as structured JSON instead of as a raw string.")]
    [DefaultValue(null)]
    public bool? ErrorsAsJson
    {
        get => GetBoolean(ErrorsAsJsonKey);
        set => SetBoolean(ErrorsAsJsonKey, value);
    }

    private string GetString(string keyword) => TryGetValue(keyword, out var value) ? value.ToString()! : "";

    private int? GetInt32(string keyword)
    {
        if (!TryGetValue(keyword, out var value))
        {
            return null;
        }

        if (value is int number)
        {
            return number;
        }

        var text = value.ToString();

        if (string.IsNullOrEmpty(text))
        {
            return null;
        }

        if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        throw new InvalidOperationException($"Connection string property '{keyword}' has value '{text}' which is not a valid integer.");
    }

    private void SetInt32(string keyword, int? value) => this[keyword] = value?.ToString(CultureInfo.InvariantCulture);

    private bool? GetBoolean(string keyword)
    {
        if (!TryGetValue(keyword, out var value))
        {
            return null;
        }

        if (value is bool flag)
        {
            return flag;
        }

        var text = value.ToString();

        if (string.IsNullOrEmpty(text))
        {
            return null;
        }

        return TryParseBoolean(text!)
               ?? throw new InvalidOperationException($"Connection string property '{keyword}' has value '{text}' which is not a valid boolean.");
    }

    /// <summary>
    /// Parses the boolean spellings DuckDB accepts when casting a string to BOOLEAN, returning null for anything else.
    /// A connection string may legitimately carry any of these, so both the typed properties and the generated
    /// configuration descriptors have to understand all of them.
    /// </summary>
    internal static bool? TryParseBoolean(string text) => text.ToLowerInvariant() switch
    {
        "true" or "1" or "yes" or "t" or "y" => true,
        "false" or "0" or "no" or "f" or "n" => false,
        _ => null
    };

    private void SetBoolean(string keyword, bool? value) => this[keyword] = value switch
    {
        true => "true",
        false => "false",
        null => null
    };

    /// <summary>
    /// Adds a descriptor for every DuckDB configuration option that does not already have a typed property, so that
    /// property grids and other <see cref="TypeDescriptor"/> consumers can discover the full option set of the
    /// native library actually in use.
    /// </summary>
    protected override void GetProperties(Hashtable propertyDescriptors)
    {
        base.GetProperties(propertyDescriptors);

        foreach (var descriptor in ConfigurationDescriptors.Value)
        {
            // The table is keyed by display name, case-insensitively. Overwriting is intended: the base class adds an
            // uncategorised, undescribed descriptor for every keyword present in the connection string.
            propertyDescriptors[descriptor.Name] = descriptor;
        }
    }

    private static PropertyDescriptor[] BuildConfigurationDescriptors()
    {
        // Types are only available from duckdb_settings(), which needs a database. Reading them opens a throwaway
        // in-memory instance, and opening a connection parses a connection string, which re-enters this class. That is
        // safe from here because the static constructor has already completed, but it would not be safe from inside it:
        // Parse would observe an empty ConfigurationOptions and throw, permanently poisoning the type. Keep this lazy.
        var optionTypes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        using (var connection = new DuckDBConnection(InMemoryConnectionString))
        {
            connection.Open();

            using var command = connection.CreateCommand();

            // name and input_type are the original columns of duckdb_settings() and have been present since the
            // function was introduced in v0.3.2, so this binds against any native library the package may be paired
            // with. scope and aliases are deliberately not requested; aliases only exists from v1.4.0 onwards.
            command.CommandText = "SELECT name, input_type FROM duckdb_settings()";

            using var reader = command.ExecuteReader();

            while (reader.Read())
            {
                optionTypes[reader.GetString(0)] = reader.GetString(1);
            }
        }

        var descriptors = new List<PropertyDescriptor>(ConfigurationOptions.Count);
        var configCount = NativeMethods.Configuration.DuckDBConfigCount();

        for (var index = 0; index < configCount; index++)
        {
            NativeMethods.Configuration.DuckDBGetConfigFlag(index, out var name, out var description);

            if (TypedPropertyKeywords.Contains(name))
            {
                continue;
            }

            optionTypes.TryGetValue(name, out var inputType);

            // List-valued options cannot be expressed in a connection string: duckdb_set_config rejects every string
            // form of a VARCHAR[] value, so offering them would only produce failures at Open time.
            if (inputType == "VARCHAR[]")
            {
                continue;
            }

            var isDebugOption = name.StartsWith("debug_", StringComparison.OrdinalIgnoreCase) || description.Contains("DEBUG SETTING");

            var attributes = new List<Attribute> { new CategoryAttribute(GetCategory(name, description, isDebugOption)) };

            // For options belonging to an extension that is not loaded, DuckDB reports the owning extension name in
            // place of a description. That makes a better category than a description, so it is used as one above.
            if (!IsExtensionName(description))
            {
                attributes.Add(new DescriptionAttribute(description));
            }

            if (isDebugOption)
            {
                attributes.Add(new BrowsableAttribute(false));
            }

            descriptors.Add(new DuckDBConfigurationOptionDescriptor(name, GetPropertyType(inputType), attributes.ToArray()));
        }

        return descriptors.ToArray();
    }

    /// <summary>
    /// Groups an option by the words in its name. Matching is done on underscore-separated segments rather than
    /// substrings, so that "catalog_error_max_schemas" is not mistaken for a logging option. Rules are used instead of
    /// a per-option table so that options added by future DuckDB versions are grouped without a change here.
    /// </summary>
    private static string GetCategory(string name, string description, bool isDebugOption)
    {
        if (isDebugOption)
        {
            return "Debug";
        }

        if (IsExtensionName(description))
        {
            return description;
        }

        if (QueryOptions.Contains(name))
        {
            return "Query";
        }

        var segments = new HashSet<string>(name.Split('_', StringSplitOptions.RemoveEmptyEntries), StringComparer.OrdinalIgnoreCase);

        bool Mentions(params string[] words) => Array.Exists(words, segments.Contains);

        if (Mentions("arrow")) return "Arrow";
        if (Mentions("profiling", "profile") || name.Contains("progress_bar", StringComparison.OrdinalIgnoreCase)) return "Profiling";
        if (Mentions("log", "logs", "logging")) return "Logging";
        if (Mentions("http", "proxy")) return "Network";
        if (Mentions("secret", "secrets", "password", "user", "username", "encryption", "filesystems")) return "Security";
        if (Mentions("extension", "extensions")) return "Extensions";

        // Checked ahead of storage so that "block_allocator_memory" is not grouped by its "block" segment.
        if (Mentions("allocator")) return "Resources";

        if (Mentions("checkpoint", "wal", "storage", "block", "compression", "zstd", "vacuum", "shredding", "variant", "parquet", "write", "partitioned")) return "Storage";
        if (Mentions("memory", "buffer", "cache", "thread", "threads")) return "Resources";
        if (Mentions("join", "joins", "threshold", "index", "pivot", "optimizer", "materialization", "aggregate", "ht", "scheduler")) return "Optimizer";

        return "General";
    }

    // DuckDB reports the owning extension ("httpfs", "mysql_scanner", ...) instead of a description for options that
    // belong to an extension. Real descriptions are prose and always contain whitespace.
    private static bool IsExtensionName(string description) => description.Length > 0 && !description.Contains(' ');

    private static Type GetPropertyType(string? inputType) => inputType switch
    {
        "BOOLEAN" => typeof(bool),
        "BIGINT" => typeof(long),
        "UBIGINT" => typeof(ulong),
        "DOUBLE" => typeof(double),
        _ => typeof(string)
    };
}