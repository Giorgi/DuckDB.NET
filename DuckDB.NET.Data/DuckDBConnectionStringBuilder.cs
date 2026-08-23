using DuckDB.NET.Data.Connection;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;

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

    private static readonly string DuckDBApi;

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
        get => base[keyword];
        set
        {
            if (DataSourceKeys.Contains(keyword) || ConfigurationOptions.Contains(keyword))
            {
                base[keyword] = value;
            }
            else
            {
                throw new InvalidOperationException($"Unrecognized connection string property '{keyword}'");
            }
        }
    }

    [Category("Connection")]
    [Description("The database to connect to.")]
    [DisplayName("DataSource")]
    public string DataSource
    {
        get
        {
            foreach (var key in DataSourceKeys)
            {
                if (TryGetValue(key, out var value))
                {
                    return value.ToString()!;
                }
            }

            return "";
        }
        set => this[DataSourceKey] = value;
    }

    [Category("Connection")]
    [Description("Access mode of the database (AUTOMATIC, READ_ONLY or READ_WRITE)")]
    [DisplayName("AccessMode")]
    public string? AccessMode
    {
        get => _accessMode;
        set
        {
            _accessMode = value;
            SetValue("access_mode", value);
        }
    }
    string? _accessMode;

    [Category("Connection")]
    [Description("The password to use. Ignored for legacy compatibility.")]
    [DisplayName("Password")]
    public string? Password
    {
        get => _password;
        set
        {
            _password = value;
            SetValue("password", value);
        }
    }
    string? _password;

    [Category("Connection")]
    [Description("The username to use. Ignored for legacy compatibility.")]
    [DisplayName("User")]
    public string? User
    {
        get => _user;
        set
        {
            _user = value;
            SetValue("user", value);
        }
    }
    string? _user;

    [Category("Connection")]
    [Description("The username to use. Ignored for legacy compatibility.")]
    [DisplayName("Username")]
    public string? Username
    {
        get => _username;
        set
        {
            _username = value;
            SetValue("username", value);
        }
    }
    string? _username;

    [Category("Arrow")]
    [Description("Whether Arrow buffers for strings, blobs, uuids and bits should be exported using large buffers")]
    [DisplayName("ArrowLargeBufferSize")]
    public bool? ArrowLargeBufferSize
    {
        get => _arrowLargeBufferSize;
        set
        {
            _arrowLargeBufferSize = value;
            SetValue("arrow_large_buffer_size", value);
        }
    }
    bool? _arrowLargeBufferSize;

    [Category("Arrow")]
    [Description("Whenever a DuckDB type does not have a clear native or canonical extension match in Arrow, export the types with a duckdb.type_name extension name.")]
    [DisplayName("ArrowLosslessConversion")]
    public bool? ArrowLosslessConversion
    {
        get => _arrowLosslessConversion;
        set
        {
            _arrowLosslessConversion = value;
            SetValue("arrow_lossless_conversion", value);
        }
    }
    bool? _arrowLosslessConversion;

    [Category("Arrow")]
    [Description("Whether export to Arrow format should use ListView as the physical layout for LIST columns")]
    [DisplayName("ArrowOutputListView")]
    public bool? ArrowOutputListView
    {
        get => _arrowOutputListView;
        set
        {
            _arrowOutputListView = value;
            SetValue("arrow_output_list_view", value);
        }
    }
    bool? _arrowOutputListView;

    [Category("Arrow")]
    [Description("Whether strings should be produced by DuckDB in Utf8View format instead of Utf8")]
    [DisplayName("ArrowOutputVersion")]
    public string? ArrowOutputVersion
    {
        get => _arrowOutputVersion;
        set
        {
            _arrowOutputVersion = value;
            SetValue("arrow_output_version", value);
        }
    }
    string? _arrowOutputVersion;

    [Category("Arrow")]
    [Description("Whether Arrow strings should be produced by DuckDB in Utf8View format instead of Utf8")]
    [DisplayName("ProduceArrowStringView")]
    public bool? ProduceArrowStringView
    {
        get => _produceArrowStringView;
        set
        {
            _produceArrowStringView = value;
            SetValue("produce_arrow_string_view", value);
        }
    }
    bool? _produceArrowStringView;

    [Category("Arrow")]
    [Description("(Experimental) Allow vacuum to compact row groups on tables with bound ART indexes, rebuilding the indexes afterward. Tables with a row count exceeding this threshold are skipped. 0 = disabled. Can also be set per-database via the 'vacuum_rebuild_indexes' ATTACH option, which overrides this default.")]
    [DisplayName("VacuumRebuildIndexes")]
    public ulong? VacuumRebuildIndexes
    {
        get => _vacuumRebuildIndexes;
        set
        {
            _vacuumRebuildIndexes = value;
            SetValue("vacuum_rebuild_indexes", value);
        }
    }
    ulong? _vacuumRebuildIndexes;

    [Category("Azure")]
    [DisplayName("AzureAccountName")]
    public string? AzureAccountName
    {
        get => _azureAccountName;
        set
        {
            _azureAccountName = value;
            SetValue("azure_account_name", value);
        }
    }
    string? _azureAccountName;

    [Category("Azure")]
    [DisplayName("AzureContextCaching")]
    public string? AzureContextCaching
    {
        get => _azureContextCaching;
        set
        {
            _azureContextCaching = value;
            SetValue("azure_context_caching", value);
        }
    }
    string? _azureContextCaching;

    [Category("Azure")]
    [DisplayName("AzureCredentialChain")]
    public string? AzureCredentialChain
    {
        get => _azureCredentialChain;
        set
        {
            _azureCredentialChain = value;
            SetValue("azure_credential_chain", value);
        }
    }
    string? _azureCredentialChain;

    [Category("Azure")]
    [DisplayName("AzureEndpoint")]
    public string? AzureEndpoint
    {
        get => _azureEndpoint;
        set
        {
            _azureEndpoint = value;
            SetValue("azure_endpoint", value);
        }
    }
    string? _azureEndpoint;

    [Category("Azure")]
    [DisplayName("AzureHttpLogging")]
    public string? AzureHttpLogging
    {
        get => _azureHttpLogging;
        set
        {
            _azureHttpLogging = value;
            SetValue("azure_http_logging", value);
        }
    }
    string? _azureHttpLogging;

    [Category("Azure")]
    [DisplayName("AzureHttpLoggingRedactHeaders")]
    public string? AzureHttpLoggingRedactHeaders
    {
        get => _azureHttpLoggingRedactHeaders;
        set
        {
            _azureHttpLoggingRedactHeaders = value;
            SetValue("azure_http_logging_redact_headers", value);
        }
    }
    string? _azureHttpLoggingRedactHeaders;

    [Category("Azure")]
    [DisplayName("AzureHttpLoggingRedactQueryParams")]
    public string? AzureHttpLoggingRedactQueryParams
    {
        get => _azureHttpLoggingRedactQueryParams;
        set
        {
            _azureHttpLoggingRedactQueryParams = value;
            SetValue("azure_http_logging_redact_query_params", value);
        }
    }
    string? _azureHttpLoggingRedactQueryParams;

    [Category("Azure")]
    [DisplayName("AzureHttpProxy")]
    public string? AzureHttpProxy
    {
        get => _azureHttpProxy;
        set
        {
            _azureHttpProxy = value;
            SetValue("azure_http_proxy", value);
        }
    }
    string? _azureHttpProxy;

    [Category("Azure")]
    [DisplayName("AzureHttpStats")]
    public string? AzureHttpStats
    {
        get => _azureHttpStats;
        set
        {
            _azureHttpStats = value;
            SetValue("azure_http_stats", value);
        }
    }
    string? _azureHttpStats;

    [Category("Azure")]
    [DisplayName("AzureProxyPassword")]
    public string? AzureProxyPassword
    {
        get => _azureProxyPassword;
        set
        {
            _azureProxyPassword = value;
            SetValue("azure_proxy_password", value);
        }
    }
    string? _azureProxyPassword;

    [Category("Azure")]
    [DisplayName("AzureProxyUserName")]
    public string? AzureProxyUserName
    {
        get => _azureProxyUserName;
        set
        {
            _azureProxyUserName = value;
            SetValue("azure_proxy_user_name", value);
        }
    }
    string? _azureProxyUserName;

    [Category("Azure")]
    [DisplayName("AzureReadBufferSize")]
    public string? AzureReadBufferSize
    {
        get => _azureReadBufferSize;
        set
        {
            _azureReadBufferSize = value;
            SetValue("azure_read_buffer_size", value);
        }
    }
    string? _azureReadBufferSize;

    [Category("Azure")]
    [DisplayName("AzureReadTransferChunkSize")]
    public string? AzureReadTransferChunkSize
    {
        get => _azureReadTransferChunkSize;
        set
        {
            _azureReadTransferChunkSize = value;
            SetValue("azure_read_transfer_chunk_size", value);
        }
    }
    string? _azureReadTransferChunkSize;

    [Category("Azure")]
    [DisplayName("AzureReadTransferConcurrency")]
    public string? AzureReadTransferConcurrency
    {
        get => _azureReadTransferConcurrency;
        set
        {
            _azureReadTransferConcurrency = value;
            SetValue("azure_read_transfer_concurrency", value);
        }
    }
    string? _azureReadTransferConcurrency;

    [Category("Azure")]
    [DisplayName("AzureStorageConnectionString")]
    public string? AzureStorageConnectionString
    {
        get => _azureStorageConnectionString;
        set
        {
            _azureStorageConnectionString = value;
            SetValue("azure_storage_connection_string", value);
        }
    }
    string? _azureStorageConnectionString;

    [Category("Azure")]
    [DisplayName("AzureTransportOptionType")]
    public string? AzureTransportOptionType
    {
        get => _azureTransportOptionType;
        set
        {
            _azureTransportOptionType = value;
            SetValue("azure_transport_option_type", value);
        }
    }
    string? _azureTransportOptionType;

    [Category("Azure")]
    [DisplayName("AzureWriteBlockSize")]
    public string? AzureWriteBlockSize
    {
        get => _azureWriteBlockSize;
        set
        {
            _azureWriteBlockSize = value;
            SetValue("azure_write_block_size", value);
        }
    }
    string? _azureWriteBlockSize;

    [Category("Azure")]
    [DisplayName("AzureWriteStagedBlocksPerCommit")]
    public string? AzureWriteStagedBlocksPerCommit
    {
        get => _azureWriteStagedBlocksPerCommit;
        set
        {
            _azureWriteStagedBlocksPerCommit = value;
            SetValue("azure_write_staged_blocks_per_commit", value);
        }
    }
    string? _azureWriteStagedBlocksPerCommit;

    [Category("Configuration")]
    [Description("Whether to enable the allocator background thread.")]
    [DisplayName("AllocatorBackgroundThreads")]
    public bool? AllocatorBackgroundThreads
    {
        get => _allocatorBackgroundThreads;
        set
        {
            _allocatorBackgroundThreads = value;
            SetValue("allocator_background_threads", value);
        }
    }
    bool? _allocatorBackgroundThreads;

    [Category("Configuration")]
    [Description("If a bulk deallocation larger than this occurs, flush outstanding allocations.")]
    [DisplayName("AllocatorBulkDeallocationFlushThreshold")]
    public string? AllocatorBulkDeallocationFlushThreshold
    {
        get => _allocatorBulkDeallocationFlushThreshold;
        set
        {
            _allocatorBulkDeallocationFlushThreshold = value;
            SetValue("allocator_bulk_deallocation_flush_threshold", value);
        }
    }
    string? _allocatorBulkDeallocationFlushThreshold;

    [Category("Configuration")]
    [Description("Peak allocation threshold at which to flush the allocator after completing a task.")]
    [DisplayName("AllocatorFlushThreshold")]
    public string? AllocatorFlushThreshold
    {
        get => _allocatorFlushThreshold;
        set
        {
            _allocatorFlushThreshold = value;
            SetValue("allocator_flush_threshold", value);
        }
    }
    string? _allocatorFlushThreshold;

    [Category("Configuration")]
    [Description("Allow to load community built extensions")]
    [DisplayName("AllowCommunityExtensions")]
    public bool? AllowCommunityExtensions
    {
        get => _allowCommunityExtensions;
        set
        {
            _allowCommunityExtensions = value;
            SetValue("allow_community_extensions", value);
        }
    }
    bool? _allowCommunityExtensions;

    [Category("Configuration")]
    [Description("Allow to load extensions with not compatible metadata")]
    [DisplayName("AllowExtensionsMetadataMismatch")]
    public bool? AllowExtensionsMetadataMismatch
    {
        get => _allowExtensionsMetadataMismatch;
        set
        {
            _allowExtensionsMetadataMismatch = value;
            SetValue("allow_extensions_metadata_mismatch", value);
        }
    }
    bool? _allowExtensionsMetadataMismatch;

    [Category("Configuration")]
    [Description("Allow extensions to override the current parser")]
    [DisplayName("AllowParserOverrideExtension")]
    public string? AllowParserOverrideExtension
    {
        get => _allowParserOverrideExtension;
        set
        {
            _allowParserOverrideExtension = value;
            SetValue("allow_parser_override_extension", value);
        }
    }
    string? _allowParserOverrideExtension;

    [Category("Configuration")]
    [Description("Allow the creation of persistent secrets, that are stored and loaded on restarts")]
    [DisplayName("AllowPersistentSecrets")]
    public bool? AllowPersistentSecrets
    {
        get => _allowPersistentSecrets;
        set
        {
            _allowPersistentSecrets = value;
            SetValue("allow_persistent_secrets", value);
        }
    }
    bool? _allowPersistentSecrets;

    [Category("Configuration")]
    [Description("Allow printing unredacted secrets")]
    [DisplayName("AllowUnredactedSecrets")]
    public bool? AllowUnredactedSecrets
    {
        get => _allowUnredactedSecrets;
        set
        {
            _allowUnredactedSecrets = value;
            SetValue("allow_unredacted_secrets", value);
        }
    }
    bool? _allowUnredactedSecrets;

    [Category("Configuration")]
    [Description("Allow to load extensions with invalid or missing signatures")]
    [DisplayName("AllowUnsignedExtensions")]
    public bool? AllowUnsignedExtensions
    {
        get => _allowUnsignedExtensions;
        set
        {
            _allowUnsignedExtensions = value;
            SetValue("allow_unsigned_extensions", value);
        }
    }
    bool? _allowUnsignedExtensions;

    [Category("Configuration")]
    [Description("List of configuration options that are ALWAYS allowed to be changed - even when lock_configuration is true")]
    [DisplayName("AllowedConfigs")]
    public string? AllowedConfigs
    {
        get => _allowedConfigs;
        set
        {
            _allowedConfigs = value;
            SetValue("allowed_configs", value);
        }
    }
    string? _allowedConfigs;

    [Category("Configuration")]
    [Description("List of directories/prefixes that are ALWAYS allowed to be queried - even when enable_external_access is false")]
    [DisplayName("AllowedDirectories")]
    public string? AllowedDirectories
    {
        get => _allowedDirectories;
        set
        {
            _allowedDirectories = value;
            SetValue("allowed_directories", value);
        }
    }
    string? _allowedDirectories;

    [Category("Configuration")]
    [Description("List of files that are ALWAYS allowed to be queried - even when enable_external_access is false")]
    [DisplayName("AllowedPaths")]
    public string? AllowedPaths
    {
        get => _allowedPaths;
        set
        {
            _allowedPaths = value;
            SetValue("allowed_paths", value);
        }
    }
    string? _allowedPaths;

    [Category("Configuration")]
    [Description("The maximum number of rows we need on the left side of an ASOF join to use a nested loop join")]
    [DisplayName("AsofLoopJoinThreshold")]
    public ulong? AsofLoopJoinThreshold
    {
        get => _asofLoopJoinThreshold;
        set
        {
            _asofLoopJoinThreshold = value;
            SetValue("asof_loop_join_threshold", value);
        }
    }
    ulong? _asofLoopJoinThreshold;

    [Category("Configuration")]
    [Description("The estimated WAL write size at which point we will skip writing to the WAL and only checkpoint. Skipping writing to the WAL means concurrent commits are blocked while the checkpoint is happening.")]
    [DisplayName("AutoCheckpointSkipWalThreshold")]
    public ulong? AutoCheckpointSkipWalThreshold
    {
        get => _autoCheckpointSkipWalThreshold;
        set
        {
            _autoCheckpointSkipWalThreshold = value;
            SetValue("auto_checkpoint_skip_wal_threshold", value);
        }
    }
    ulong? _autoCheckpointSkipWalThreshold;

    [Category("Configuration")]
    [Description("Overrides the custom endpoint for extension installation on autoloading")]
    [DisplayName("AutoinstallExtensionRepository")]
    public string? AutoinstallExtensionRepository
    {
        get => _autoinstallExtensionRepository;
        set
        {
            _autoinstallExtensionRepository = value;
            SetValue("autoinstall_extension_repository", value);
        }
    }
    string? _autoinstallExtensionRepository;

    [Category("Configuration")]
    [Description("Whether known extensions are allowed to be automatically installed when a query depends on them")]
    [DisplayName("AutoinstallKnownExtensions")]
    public bool? AutoinstallKnownExtensions
    {
        get => _autoinstallKnownExtensions;
        set
        {
            _autoinstallKnownExtensions = value;
            SetValue("autoinstall_known_extensions", value);
        }
    }
    bool? _autoinstallKnownExtensions;

    [Category("Configuration")]
    [Description("Whether known extensions are allowed to be automatically loaded when a query depends on them")]
    [DisplayName("AutoloadKnownExtensions")]
    public bool? AutoloadKnownExtensions
    {
        get => _autoloadKnownExtensions;
        set
        {
            _autoloadKnownExtensions = value;
            SetValue("autoload_known_extensions", value);
        }
    }
    bool? _autoloadKnownExtensions;

    [Category("Configuration")]
    [Description("Physical memory that the block allocator is allowed to use (this memory is never freed and cannot be reduced).")]
    [DisplayName("BlockAllocatorMemory")]
    public string? BlockAllocatorMemory
    {
        get => _blockAllocatorMemory;
        set
        {
            _blockAllocatorMemory = value;
            SetValue("block_allocator_memory", value);
        }
    }
    string? _blockAllocatorMemory;

    [Category("Configuration")]
    [Description("The maximum number of schemas the system will scan for \"did you mean...\" style errors in the catalog")]
    [DisplayName("CatalogErrorMaxSchemas")]
    public ulong? CatalogErrorMaxSchemas
    {
        get => _catalogErrorMaxSchemas;
        set
        {
            _catalogErrorMaxSchemas = value;
            SetValue("catalog_error_max_schemas", value);
        }
    }
    ulong? _catalogErrorMaxSchemas;

    [Category("Configuration")]
    [Description("The WAL size threshold at which to automatically trigger a checkpoint (e.g. 1GB)")]
    [DisplayName("CheckpointThreshold")]
    public string? CheckpointThreshold
    {
        get => _checkpointThreshold;
        set
        {
            _checkpointThreshold = value;
            SetValue("checkpoint_threshold", value);
        }
    }
    string? _checkpointThreshold;

    [Category("Configuration")]
    [Description("Which types of exceptions invalidate the database for the current transaction")]
    [DisplayName("CurrentTransactionInvalidationPolicy")]
    public string? CurrentTransactionInvalidationPolicy
    {
        get => _currentTransactionInvalidationPolicy;
        set
        {
            _currentTransactionInvalidationPolicy = value;
            SetValue("current_transaction_invalidation_policy", value);
        }
    }
    string? _currentTransactionInvalidationPolicy;

    [Category("Configuration")]
    [Description("Overrides the custom endpoint for remote extension installation")]
    [DisplayName("CustomExtensionRepository")]
    public string? CustomExtensionRepository
    {
        get => _customExtensionRepository;
        set
        {
            _customExtensionRepository = value;
            SetValue("custom_extension_repository", value);
        }
    }
    string? _customExtensionRepository;

    [Category("Configuration")]
    [Description("Accepts a JSON enabling custom metrics")]
    [DisplayName("CustomProfilingSettings")]
    public string? CustomProfilingSettings
    {
        get => _customProfilingSettings;
        set
        {
            _customProfilingSettings = value;
            SetValue("custom_profiling_settings", value);
        }
    }
    string? _customProfilingSettings;

    [Category("Configuration")]
    [Description("Metadata from DuckDB callers")]
    [DisplayName("CustomUserAgent")]
    public string? CustomUserAgent
    {
        get => _customUserAgent;
        set
        {
            _customUserAgent = value;
            SetValue("custom_user_agent", value);
        }
    }
    string? _customUserAgent;

    [Category("Configuration")]
    [Description("The default block size for new duckdb database files (new as-in, they do not yet exist).")]
    [DisplayName("DefaultBlockSize")]
    public ulong? DefaultBlockSize
    {
        get => _defaultBlockSize;
        set
        {
            _defaultBlockSize = value;
            SetValue("default_block_size", value);
        }
    }
    ulong? _defaultBlockSize;

    [Category("Configuration")]
    [Description("The collation setting used when none is specified")]
    [DisplayName("DefaultCollation")]
    public string? DefaultCollation
    {
        get => _defaultCollation;
        set
        {
            _defaultCollation = value;
            SetValue("default_collation", value);
        }
    }
    string? _defaultCollation;

    [Category("Configuration")]
    [Description("NULL ordering used when none is specified (NULLS_FIRST or NULLS_LAST)")]
    [DisplayName("DefaultNullOrder")]
    public string? DefaultNullOrder
    {
        get => _defaultNullOrder;
        set
        {
            _defaultNullOrder = value;
            SetValue("default_null_order", value);
        }
    }
    string? _defaultNullOrder;

    [Category("Configuration")]
    [Description("The order type used when none is specified (ASC or DESC)")]
    [DisplayName("DefaultOrder")]
    public string? DefaultOrder
    {
        get => _defaultOrder;
        set
        {
            _defaultOrder = value;
            SetValue("default_order", value);
        }
    }
    string? _defaultOrder;

    [Category("Configuration")]
    [Description("Allows switching the default storage for secrets")]
    [DisplayName("DefaultSecretStorage")]
    public string? DefaultSecretStorage
    {
        get => _defaultSecretStorage;
        set
        {
            _defaultSecretStorage = value;
            SetValue("default_secret_storage", value);
        }
    }
    string? _defaultSecretStorage;

    [Category("Configuration")]
    [Description("Configures the use of the deprecated union syntax for USING KEY CTEs.")]
    [DisplayName("DeprecatedUsingKeySyntax")]
    public string? DeprecatedUsingKeySyntax
    {
        get => _deprecatedUsingKeySyntax;
        set
        {
            _deprecatedUsingKeySyntax = value;
            SetValue("deprecated_using_key_syntax", value);
        }
    }
    string? _deprecatedUsingKeySyntax;

    [Category("Configuration")]
    [Description("Disables invalidating the database instance when encountering a fatal error. Should be used with great care, as DuckDB cannot guarantee correct behavior after a fatal error.")]
    [DisplayName("DisableDatabaseInvalidation")]
    public bool? DisableDatabaseInvalidation
    {
        get => _disableDatabaseInvalidation;
        set
        {
            _disableDatabaseInvalidation = value;
            SetValue("disable_database_invalidation", value);
        }
    }
    bool? _disableDatabaseInvalidation;

    [Category("Configuration")]
    [Description("Disable casting from timestamp to timestamptz ")]
    [DisplayName("DisableTimestamptzCasts")]
    public bool? DisableTimestamptzCasts
    {
        get => _disableTimestamptzCasts;
        set
        {
            _disableTimestamptzCasts = value;
            SetValue("disable_timestamptz_casts", value);
        }
    }
    bool? _disableTimestamptzCasts;

    [Category("Configuration")]
    [Description("Disable a specific set of compression methods (comma separated)")]
    [DisplayName("DisabledCompressionMethods")]
    public string? DisabledCompressionMethods
    {
        get => _disabledCompressionMethods;
        set
        {
            _disabledCompressionMethods = value;
            SetValue("disabled_compression_methods", value);
        }
    }
    string? _disabledCompressionMethods;

    [Category("Configuration")]
    [Description("Disable specific file systems preventing access (e.g. LocalFileSystem)")]
    [DisplayName("DisabledFilesystems")]
    public string? DisabledFilesystems
    {
        get => _disabledFilesystems;
        set
        {
            _disabledFilesystems = value;
            SetValue("disabled_filesystems", value);
        }
    }
    string? _disabledFilesystems;

    [Category("Configuration")]
    [Description("Sets the list of disabled loggers")]
    [DisplayName("DisabledLogTypes")]
    public string? DisabledLogTypes
    {
        get => _disabledLogTypes;
        set
        {
            _disabledLogTypes = value;
            SetValue("disabled_log_types", value);
        }
    }
    string? _disabledLogTypes;

    [Category("Configuration")]
    [Description("DuckDB API surface")]
    [DisplayName("DuckdbApi")]
    public string? DuckdbApi
    {
        get => _duckdbApi;
        set
        {
            _duckdbApi = value;
            SetValue("duckdb_api", value);
        }
    }
    string? _duckdbApi;

    [Category("Configuration")]
    [Description("The maximum amount of OR filters we generate dynamically from a hash join")]
    [DisplayName("DynamicOrFilterThreshold")]
    public ulong? DynamicOrFilterThreshold
    {
        get => _dynamicOrFilterThreshold;
        set
        {
            _dynamicOrFilterThreshold = value;
            SetValue("dynamic_or_filter_threshold", value);
        }
    }
    ulong? _dynamicOrFilterThreshold;

    [Category("Configuration")]
    [Description("Enables caching operators that cache intermediate results")]
    [DisplayName("EnableCachingOperators")]
    public bool? EnableCachingOperators
    {
        get => _enableCachingOperators;
        set
        {
            _enableCachingOperators = value;
            SetValue("enable_caching_operators", value);
        }
    }
    bool? _enableCachingOperators;

    [Category("Configuration")]
    [Description("Allow the database to access external state (through e.g. loading/installing modules, COPY TO/FROM, CSV readers, pandas replacement scans, etc)")]
    [DisplayName("EnableExternalAccess")]
    public bool? EnableExternalAccess
    {
        get => _enableExternalAccess;
        set
        {
            _enableExternalAccess = value;
            SetValue("enable_external_access", value);
        }
    }
    bool? _enableExternalAccess;

    [Category("Configuration")]
    [Description("Allow the database to cache external files (e.g., Parquet) in memory.")]
    [DisplayName("EnableExternalFileCache")]
    public bool? EnableExternalFileCache
    {
        get => _enableExternalFileCache;
        set
        {
            _enableExternalFileCache = value;
            SetValue("enable_external_file_cache", value);
        }
    }
    bool? _enableExternalFileCache;

    [Category("Configuration")]
    [Description("Allow scans on FSST compressed segments to emit compressed vectors to utilize late decompression")]
    [DisplayName("EnableFsstVectors")]
    public bool? EnableFsstVectors
    {
        get => _enableFsstVectors;
        set
        {
            _enableFsstVectors = value;
            SetValue("enable_fsst_vectors", value);
        }
    }
    bool? _enableFsstVectors;

    [Category("Configuration")]
    [Description("(deprecated) Enables HTTP logging")]
    [DisplayName("EnableHttpLogging")]
    public bool? EnableHttpLogging
    {
        get => _enableHttpLogging;
        set
        {
            _enableHttpLogging = value;
            SetValue("enable_http_logging", value);
        }
    }
    bool? _enableHttpLogging;

    [Category("Configuration")]
    [Description("Whether or not the global http metadata is used to cache HTTP metadata")]
    [DisplayName("EnableHttpMetadataCache")]
    public bool? EnableHttpMetadataCache
    {
        get => _enableHttpMetadataCache;
        set
        {
            _enableHttpMetadataCache = value;
            SetValue("enable_http_metadata_cache", value);
        }
    }
    bool? _enableHttpMetadataCache;

    [Category("Configuration")]
    [Description("Enables the logger")]
    [DisplayName("EnableLogging")]
    public bool? EnableLogging
    {
        get => _enableLogging;
        set
        {
            _enableLogging = value;
            SetValue("enable_logging", value);
        }
    }
    bool? _enableLogging;

    [Category("Configuration")]
    [Description("Enable created MACROs to create dependencies on the referenced objects (such as tables)")]
    [DisplayName("EnableMacroDependencies")]
    public bool? EnableMacroDependencies
    {
        get => _enableMacroDependencies;
        set
        {
            _enableMacroDependencies = value;
            SetValue("enable_macro_dependencies", value);
        }
    }
    bool? _enableMacroDependencies;

    [Category("Configuration")]
    [Description("[PLACEHOLDER] Legacy setting - does nothing")]
    [DisplayName("EnableObjectCache")]
    public bool? EnableObjectCache
    {
        get => _enableObjectCache;
        set
        {
            _enableObjectCache = value;
            SetValue("enable_object_cache", value);
        }
    }
    bool? _enableObjectCache;

    [Category("Configuration")]
    [Description("Enables profiling, and sets the output format (JSON, QUERY_TREE, QUERY_TREE_OPTIMIZER)")]
    [DisplayName("EnableProfiling")]
    public string? EnableProfiling
    {
        get => _enableProfiling;
        set
        {
            _enableProfiling = value;
            SetValue("enable_profiling", value);
        }
    }
    string? _enableProfiling;

    [Category("Configuration")]
    [Description("Enables the progress bar, printing progress to the terminal for long queries")]
    [DisplayName("EnableProgressBar")]
    public string? EnableProgressBar
    {
        get => _enableProgressBar;
        set
        {
            _enableProgressBar = value;
            SetValue("enable_progress_bar", value);
        }
    }
    string? _enableProgressBar;

    [Category("Configuration")]
    [Description("Controls the printing of the progress bar, when 'enable_progress_bar' is true")]
    [DisplayName("EnableProgressBarPrint")]
    public bool? EnableProgressBarPrint
    {
        get => _enableProgressBarPrint;
        set
        {
            _enableProgressBarPrint = value;
            SetValue("enable_progress_bar_print", value);
        }
    }
    bool? _enableProgressBarPrint;

    [Category("Configuration")]
    [Description("Enable created VIEWs to create dependencies on the referenced objects (such as tables)")]
    [DisplayName("EnableViewDependencies")]
    public bool? EnableViewDependencies
    {
        get => _enableViewDependencies;
        set
        {
            _enableViewDependencies = value;
            SetValue("enable_view_dependencies", value);
        }
    }
    bool? _enableViewDependencies;

    [Category("Configuration")]
    [Description("Sets the list of enabled loggers")]
    [DisplayName("EnabledLogTypes")]
    public string? EnabledLogTypes
    {
        get => _enabledLogTypes;
        set
        {
            _enabledLogTypes = value;
            SetValue("enabled_log_types", value);
        }
    }
    string? _enabledLogTypes;

    [Category("Configuration")]
    [Description("Output error messages as structured JSON instead of as a raw string")]
    [DisplayName("ErrorsAsJson")]
    public bool? ErrorsAsJson
    {
        get => _errorsAsJson;
        set
        {
            _errorsAsJson = value;
            SetValue("errors_as_json", value);
        }
    }
    bool? _errorsAsJson;

    [Category("Configuration")]
    [Description("EXPERIMENTAL: Re-use row group and table metadata when checkpointing.")]
    [DisplayName("ExperimentalMetadataReuse")]
    public bool? ExperimentalMetadataReuse
    {
        get => _experimentalMetadataReuse;
        set
        {
            _experimentalMetadataReuse = value;
            SetValue("experimental_metadata_reuse", value);
        }
    }
    bool? _experimentalMetadataReuse;

    [Category("Configuration")]
    [Description("Output of EXPLAIN statements (ALL, OPTIMIZED_ONLY, PHYSICAL_ONLY)")]
    [DisplayName("ExplainOutput")]
    public string? ExplainOutput
    {
        get => _explainOutput;
        set
        {
            _explainOutput = value;
            SetValue("explain_output", value);
        }
    }
    string? _explainOutput;

    [Category("Configuration")]
    [Description("Set the directories to store extensions in")]
    [DisplayName("ExtensionDirectories")]
    public string? ExtensionDirectories
    {
        get => _extensionDirectories;
        set
        {
            _extensionDirectories = value;
            SetValue("extension_directories", value);
        }
    }
    string? _extensionDirectories;

    [Category("Configuration")]
    [Description("Set the directory to store extensions in")]
    [DisplayName("ExtensionDirectory")]
    public string? ExtensionDirectory
    {
        get => _extensionDirectory;
        set
        {
            _extensionDirectory = value;
            SetValue("extension_directory", value);
        }
    }
    string? _extensionDirectory;

    [Category("Configuration")]
    [Description("The number of external threads that work on DuckDB tasks.")]
    [DisplayName("ExternalThreads")]
    public ulong? ExternalThreads
    {
        get => _externalThreads;
        set
        {
            _externalThreads = value;
            SetValue("external_threads", value);
        }
    }
    ulong? _externalThreads;

    [Category("Configuration")]
    [Description("A comma separated list of directories to search for input files")]
    [DisplayName("FileSearchPath")]
    public string? FileSearchPath
    {
        get => _fileSearchPath;
        set
        {
            _fileSearchPath = value;
            SetValue("file_search_path", value);
        }
    }
    string? _fileSearchPath;

    [Category("Configuration")]
    [Description("Force re-use of row group metadata on a column-level when checkpointing on older storage versions 6 and 7. This breaks storage backward-compatibility with older DuckDB versions.")]
    [DisplayName("ForceColumnMetadataReuse")]
    public string? ForceColumnMetadataReuse
    {
        get => _forceColumnMetadataReuse;
        set
        {
            _forceColumnMetadataReuse = value;
            SetValue("force_column_metadata_reuse", value);
        }
    }
    string? _forceColumnMetadataReuse;

    [Category("Configuration")]
    [Description("Enable mbedtls for encryption (WARNING: unsafe to use)")]
    [DisplayName("ForceMbedtlsUnsafe")]
    public bool? ForceMbedtlsUnsafe
    {
        get => _forceMbedtlsUnsafe;
        set
        {
            _forceMbedtlsUnsafe = value;
            SetValue("force_mbedtls_unsafe", value);
        }
    }
    bool? _forceMbedtlsUnsafe;

    [Category("Configuration")]
    [Description("Forces the VARIANT shredding that happens at checkpoint to use the provided schema for the shredding.")]
    [DisplayName("ForceVariantShredding")]
    public string? ForceVariantShredding
    {
        get => _forceVariantShredding;
        set
        {
            _forceVariantShredding = value;
            SetValue("force_variant_shredding", value);
        }
    }
    string? _forceVariantShredding;

    [Category("Configuration")]
    [Description("Minimum size of a rowgroup to enable GEOMETRY shredding, or set to -1 to disable entirely. Defaults to 1/4th of a rowgroup")]
    [DisplayName("GeometryMinimumShreddingSize")]
    public ulong? GeometryMinimumShreddingSize
    {
        get => _geometryMinimumShreddingSize;
        set
        {
            _geometryMinimumShreddingSize = value;
            SetValue("geometry_minimum_shredding_size", value);
        }
    }
    ulong? _geometryMinimumShreddingSize;

    [Category("Configuration")]
    [Description("Sets the home directory used by the system")]
    [DisplayName("HomeDirectory")]
    public string? HomeDirectory
    {
        get => _homeDirectory;
        set
        {
            _homeDirectory = value;
            SetValue("home_directory", value);
        }
    }
    string? _homeDirectory;

    [Category("Configuration")]
    [Description("(deprecated) The file to which HTTP logging output should be saved, or empty to print to the terminal")]
    [DisplayName("HttpLoggingOutput")]
    public string? HttpLoggingOutput
    {
        get => _httpLoggingOutput;
        set
        {
            _httpLoggingOutput = value;
            SetValue("http_logging_output", value);
        }
    }
    string? _httpLoggingOutput;

    [Category("Configuration")]
    [Description("HTTP proxy host (defaults to the HTTP_PROXY environment variable when unset)")]
    [DisplayName("HttpProxy")]
    public string? HttpProxy
    {
        get => _httpProxy;
        set
        {
            _httpProxy = value;
            SetValue("http_proxy", value);
        }
    }
    string? _httpProxy;

    [Category("Configuration")]
    [Description("Password for HTTP proxy")]
    [DisplayName("HttpProxyPassword")]
    public string? HttpProxyPassword
    {
        get => _httpProxyPassword;
        set
        {
            _httpProxyPassword = value;
            SetValue("http_proxy_password", value);
        }
    }
    string? _httpProxyPassword;

    [Category("Configuration")]
    [Description("Username for HTTP proxy")]
    [DisplayName("HttpProxyUsername")]
    public string? HttpProxyUsername
    {
        get => _httpProxyUsername;
        set
        {
            _httpProxyUsername = value;
            SetValue("http_proxy_username", value);
        }
    }
    string? _httpProxyUsername;

    [Category("Configuration")]
    [Description("Use IEE754-compliant floating point operations (returning NAN instead of errors/NULL).")]
    [DisplayName("IeeeFloatingPointOps")]
    public bool? IeeeFloatingPointOps
    {
        get => _ieeeFloatingPointOps;
        set
        {
            _ieeeFloatingPointOps = value;
            SetValue("ieee_floating_point_ops", value);
        }
    }
    bool? _ieeeFloatingPointOps;

    [Category("Configuration")]
    [Description("Ignore unknown Coordinate Reference Systems (CRS) when creating geometry types or importing geospatial data.")]
    [DisplayName("IgnoreUnknownCrs")]
    public bool? IgnoreUnknownCrs
    {
        get => _ignoreUnknownCrs;
        set
        {
            _ignoreUnknownCrs = value;
            SetValue("ignore_unknown_crs", value);
        }
    }
    bool? _ignoreUnknownCrs;

    [Category("Configuration")]
    [Description("Whether transactions should be started lazily when needed, or immediately when BEGIN TRANSACTION is called")]
    [DisplayName("ImmediateTransactionMode")]
    public bool? ImmediateTransactionMode
    {
        get => _immediateTransactionMode;
        set
        {
            _immediateTransactionMode = value;
            SetValue("immediate_transaction_mode", value);
        }
    }
    bool? _immediateTransactionMode;

    [Category("Configuration")]
    [Description("The maximum index scan count sets a threshold for index scans. If fewer than MAX(index_scan_max_count, index_scan_percentage * total_row_count) rows match, we perform an index scan instead of a table scan.")]
    [DisplayName("IndexScanMaxCount")]
    public ulong? IndexScanMaxCount
    {
        get => _indexScanMaxCount;
        set
        {
            _indexScanMaxCount = value;
            SetValue("index_scan_max_count", value);
        }
    }
    ulong? _indexScanMaxCount;

    [Category("Configuration")]
    [Description("The index scan percentage sets a threshold for index scans. If fewer than MAX(index_scan_max_count, index_scan_percentage * total_row_count) rows match, we perform an index scan instead of a table scan.")]
    [DisplayName("IndexScanPercentage")]
    public double? IndexScanPercentage
    {
        get => _indexScanPercentage;
        set
        {
            _indexScanPercentage = value;
            SetValue("index_scan_percentage", value);
        }
    }
    double? _indexScanPercentage;

    [Category("Configuration")]
    [Description("Whether or not the / operator defaults to integer division, or to floating point division")]
    [DisplayName("IntegerDivision")]
    public bool? IntegerDivision
    {
        get => _integerDivision;
        set
        {
            _integerDivision = value;
            SetValue("integer_division", value);
        }
    }
    bool? _integerDivision;

    [Category("Configuration")]
    [Description("Configures the use of the deprecated single arrow operator (->) for lambda functions.")]
    [DisplayName("LambdaSyntax")]
    public string? LambdaSyntax
    {
        get => _lambdaSyntax;
        set
        {
            _lambdaSyntax = value;
            SetValue("lambda_syntax", value);
        }
    }
    string? _lambdaSyntax;

    [Category("Configuration")]
    [Description("The maximum amount of rows in the LIMIT/SAMPLE for which we trigger late materialization")]
    [DisplayName("LateMaterializationMaxRows")]
    public ulong? LateMaterializationMaxRows
    {
        get => _lateMaterializationMaxRows;
        set
        {
            _lateMaterializationMaxRows = value;
            SetValue("late_materialization_max_rows", value);
        }
    }
    ulong? _lateMaterializationMaxRows;

    [Category("Configuration")]
    [Description("Whether or not configurations can be altered")]
    [DisplayName("LockConfiguration")]
    public bool? LockConfiguration
    {
        get => _lockConfiguration;
        set
        {
            _lockConfiguration = value;
            SetValue("lock_configuration", value);
        }
    }
    bool? _lockConfiguration;

    [Category("Configuration")]
    [Description("Specifies the path to which queries should be logged (default: NULL, queries are not logged)")]
    [DisplayName("LogQueryPath")]
    public string? LogQueryPath
    {
        get => _logQueryPath;
        set
        {
            _logQueryPath = value;
            SetValue("log_query_path", value);
        }
    }
    string? _logQueryPath;

    [Category("Configuration")]
    [Description("The log level which will be recorded in the log")]
    [DisplayName("LoggingLevel")]
    public string? LoggingLevel
    {
        get => _loggingLevel;
        set
        {
            _loggingLevel = value;
            SetValue("logging_level", value);
        }
    }
    string? _loggingLevel;

    [Category("Configuration")]
    [Description("Determines which types of log messages are logged")]
    [DisplayName("LoggingMode")]
    public string? LoggingMode
    {
        get => _loggingMode;
        set
        {
            _loggingMode = value;
            SetValue("logging_mode", value);
        }
    }
    string? _loggingMode;

    [Category("Configuration")]
    [Description("Set the logging storage (memory/stdout/file/<custom>)")]
    [DisplayName("LoggingStorage")]
    public string? LoggingStorage
    {
        get => _loggingStorage;
        set
        {
            _loggingStorage = value;
            SetValue("logging_storage", value);
        }
    }
    string? _loggingStorage;

    [Category("Configuration")]
    [Description("The maximum expression depth limit in the parser. WARNING: increasing this setting and using very deep expressions might lead to stack overflow errors.")]
    [DisplayName("MaxExpressionDepth")]
    public ulong? MaxExpressionDepth
    {
        get => _maxExpressionDepth;
        set
        {
            _maxExpressionDepth = value;
            SetValue("max_expression_depth", value);
        }
    }
    ulong? _maxExpressionDepth;

    [Category("Configuration")]
    [Description("The maximum memory of the system (e.g. 1GB)")]
    [DisplayName("MaxMemory")]
    public string? MaxMemory
    {
        get => _maxMemory;
        set
        {
            _maxMemory = value;
            SetValue("max_memory", value);
        }
    }
    string? _maxMemory;

    [Category("Configuration")]
    [Description("The maximum amount of data stored inside the 'temp_directory' (when set) (e.g. 1GB)")]
    [DisplayName("MaxTempDirectorySize")]
    public string? MaxTempDirectorySize
    {
        get => _maxTempDirectorySize;
        set
        {
            _maxTempDirectorySize = value;
            SetValue("max_temp_directory_size", value);
        }
    }
    string? _maxTempDirectorySize;

    [Category("Configuration")]
    [Description("The maximum vacuum tasks to schedule during a checkpoint.")]
    [DisplayName("MaxVacuumTasks")]
    public ulong? MaxVacuumTasks
    {
        get => _maxVacuumTasks;
        set
        {
            _maxVacuumTasks = value;
            SetValue("max_vacuum_tasks", value);
        }
    }
    ulong? _maxVacuumTasks;

    [Category("Configuration")]
    [Description("The maximum memory of the system (e.g. 1GB)")]
    [DisplayName("MemoryLimit")]
    public string? MemoryLimit
    {
        get => _memoryLimit;
        set
        {
            _memoryLimit = value;
            SetValue("memory_limit", value);
        }
    }
    string? _memoryLimit;

    [Category("Configuration")]
    [Description("The maximum number of rows on either table to choose a merge join")]
    [DisplayName("MergeJoinThreshold")]
    public ulong? MergeJoinThreshold
    {
        get => _mergeJoinThreshold;
        set
        {
            _mergeJoinThreshold = value;
            SetValue("merge_join_threshold", value);
        }
    }
    ulong? _mergeJoinThreshold;

    [Category("Configuration")]
    [Description("The maximum number of rows on either table to choose a nested loop join")]
    [DisplayName("NestedLoopJoinThreshold")]
    public ulong? NestedLoopJoinThreshold
    {
        get => _nestedLoopJoinThreshold;
        set
        {
            _nestedLoopJoinThreshold = value;
            SetValue("nested_loop_join_threshold", value);
        }
    }
    ulong? _nestedLoopJoinThreshold;

    [Category("Configuration")]
    [Description("NULL ordering used when none is specified (NULLS_FIRST or NULLS_LAST)")]
    [DisplayName("NullOrder")]
    public string? NullOrder
    {
        get => _nullOrder;
        set
        {
            _nullOrder = value;
            SetValue("null_order", value);
        }
    }
    string? _nullOrder;

    [Category("Configuration")]
    [Description("Allow implicit casting to/from VARCHAR")]
    [DisplayName("OldImplicitCasting")]
    public string? OldImplicitCasting
    {
        get => _oldImplicitCasting;
        set
        {
            _oldImplicitCasting = value;
            SetValue("old_implicit_casting", value);
        }
    }
    string? _oldImplicitCasting;

    [Category("Configuration")]
    [Description("Allow ordering by non-integer literals - ordering by such literals has no effect.")]
    [DisplayName("OrderByNonIntegerLiteral")]
    public bool? OrderByNonIntegerLiteral
    {
        get => _orderByNonIntegerLiteral;
        set
        {
            _orderByNonIntegerLiteral = value;
            SetValue("order_by_non_integer_literal", value);
        }
    }
    bool? _orderByNonIntegerLiteral;

    [Category("Configuration")]
    [Description("The number of rows to accumulate before sorting, used for tuning")]
    [DisplayName("OrderedAggregateThreshold")]
    public ulong? OrderedAggregateThreshold
    {
        get => _orderedAggregateThreshold;
        set
        {
            _orderedAggregateThreshold = value;
            SetValue("ordered_aggregate_threshold", value);
        }
    }
    ulong? _orderedAggregateThreshold;

    [Category("Configuration")]
    [Description("The threshold in number of rows after which we flush a thread state when writing using PARTITION_BY")]
    [DisplayName("PartitionedWriteFlushThreshold")]
    public ulong? PartitionedWriteFlushThreshold
    {
        get => _partitionedWriteFlushThreshold;
        set
        {
            _partitionedWriteFlushThreshold = value;
            SetValue("partitioned_write_flush_threshold", value);
        }
    }
    ulong? _partitionedWriteFlushThreshold;

    [Category("Configuration")]
    [Description("The maximum amount of files the system can keep open before flushing to disk when writing using PARTITION_BY")]
    [DisplayName("PartitionedWriteMaxOpenFiles")]
    public ulong? PartitionedWriteMaxOpenFiles
    {
        get => _partitionedWriteMaxOpenFiles;
        set
        {
            _partitionedWriteMaxOpenFiles = value;
            SetValue("partitioned_write_max_open_files", value);
        }
    }
    ulong? _partitionedWriteMaxOpenFiles;

    [Category("Configuration")]
    [Description("Threshold in bytes for when to use a perfect hash table")]
    [DisplayName("PerfectHtThreshold")]
    public ulong? PerfectHtThreshold
    {
        get => _perfectHtThreshold;
        set
        {
            _perfectHtThreshold = value;
            SetValue("perfect_ht_threshold", value);
        }
    }
    ulong? _perfectHtThreshold;

    [Category("Configuration")]
    [Description("Whether to pin threads to cores (Linux only, default AUTO: on when there are more than 64 cores)")]
    [DisplayName("PinThreads")]
    public string? PinThreads
    {
        get => _pinThreads;
        set
        {
            _pinThreads = value;
            SetValue("pin_threads", value);
        }
    }
    string? _pinThreads;

    [Category("Configuration")]
    [Description("The threshold to switch from using filtered aggregates to LIST with a dedicated pivot operator")]
    [DisplayName("PivotFilterThreshold")]
    public ulong? PivotFilterThreshold
    {
        get => _pivotFilterThreshold;
        set
        {
            _pivotFilterThreshold = value;
            SetValue("pivot_filter_threshold", value);
        }
    }
    ulong? _pivotFilterThreshold;

    [Category("Configuration")]
    [Description("The maximum number of pivot columns in a pivot statement")]
    [DisplayName("PivotLimit")]
    public ulong? PivotLimit
    {
        get => _pivotLimit;
        set
        {
            _pivotLimit = value;
            SetValue("pivot_limit", value);
        }
    }
    ulong? _pivotLimit;

    [Category("Configuration")]
    [Description("Force use of range joins with mixed predicates")]
    [DisplayName("PreferRangeJoins")]
    public bool? PreferRangeJoins
    {
        get => _preferRangeJoins;
        set
        {
            _preferRangeJoins = value;
            SetValue("prefer_range_joins", value);
        }
    }
    bool? _preferRangeJoins;

    [Category("Configuration")]
    [Description("Whether or not to preserve the identifier case, instead of always lowercasing all non-quoted identifiers")]
    [DisplayName("PreserveIdentifierCase")]
    public bool? PreserveIdentifierCase
    {
        get => _preserveIdentifierCase;
        set
        {
            _preserveIdentifierCase = value;
            SetValue("preserve_identifier_case", value);
        }
    }
    bool? _preserveIdentifierCase;

    [Category("Configuration")]
    [Description("Whether or not to preserve insertion order. If set to false the system is allowed to re-order any results that do not contain ORDER BY clauses.")]
    [DisplayName("PreserveInsertionOrder")]
    public bool? PreserveInsertionOrder
    {
        get => _preserveInsertionOrder;
        set
        {
            _preserveInsertionOrder = value;
            SetValue("preserve_insertion_order", value);
        }
    }
    bool? _preserveInsertionOrder;

    [Category("Configuration")]
    [Description("The file to which profile output should be saved, or empty to print to the terminal")]
    [DisplayName("ProfileOutput")]
    public string? ProfileOutput
    {
        get => _profileOutput;
        set
        {
            _profileOutput = value;
            SetValue("profile_output", value);
        }
    }
    string? _profileOutput;

    [Category("Configuration")]
    [Description("The profiling coverage (SELECT or ALL)")]
    [DisplayName("ProfilingCoverage")]
    public string? ProfilingCoverage
    {
        get => _profilingCoverage;
        set
        {
            _profilingCoverage = value;
            SetValue("profiling_coverage", value);
        }
    }
    string? _profilingCoverage;

    [Category("Configuration")]
    [Description("The profiling mode (STANDARD or DETAILED)")]
    [DisplayName("ProfilingMode")]
    public string? ProfilingMode
    {
        get => _profilingMode;
        set
        {
            _profilingMode = value;
            SetValue("profiling_mode", value);
        }
    }
    string? _profilingMode;

    [Category("Configuration")]
    [Description("The file to which profile output should be saved, or empty to print to the terminal")]
    [DisplayName("ProfilingOutput")]
    public string? ProfilingOutput
    {
        get => _profilingOutput;
        set
        {
            _profilingOutput = value;
            SetValue("profiling_output", value);
        }
    }
    string? _profilingOutput;

    [Category("Configuration")]
    [Description("Sets the time (in milliseconds) how long a query needs to take before we start printing a progress bar")]
    [DisplayName("ProgressBarTime")]
    public ulong? ProgressBarTime
    {
        get => _progressBarTime;
        set
        {
            _progressBarTime = value;
            SetValue("progress_bar_time", value);
        }
    }
    ulong? _progressBarTime;

    [Category("Configuration")]
    [Description("When a scalar subquery returns multiple rows - return a random row instead of returning an error.")]
    [DisplayName("ScalarSubqueryErrorOnMultipleRows")]
    public bool? ScalarSubqueryErrorOnMultipleRows
    {
        get => _scalarSubqueryErrorOnMultipleRows;
        set
        {
            _scalarSubqueryErrorOnMultipleRows = value;
            SetValue("scalar_subquery_error_on_multiple_rows", value);
        }
    }
    bool? _scalarSubqueryErrorOnMultipleRows;

    [Category("Configuration")]
    [Description("Partially process tasks before rescheduling - allows for more scheduler fairness between separate queries")]
    [DisplayName("SchedulerProcessPartial")]
    public bool? SchedulerProcessPartial
    {
        get => _schedulerProcessPartial;
        set
        {
            _schedulerProcessPartial = value;
            SetValue("scheduler_process_partial", value);
        }
    }
    bool? _schedulerProcessPartial;

    [Category("Configuration")]
    [Description("Sets the default search schema. Equivalent to setting search_path to a single value.")]
    [DisplayName("Schema")]
    public string? Schema
    {
        get => _schema;
        set
        {
            _schema = value;
            SetValue("schema", value);
        }
    }
    string? _schema;

    [Category("Configuration")]
    [Description("Sets the default catalog search path as a comma-separated list of values")]
    [DisplayName("SearchPath")]
    public string? SearchPath
    {
        get => _searchPath;
        set
        {
            _searchPath = value;
            SetValue("search_path", value);
        }
    }
    string? _searchPath;

    [Category("Configuration")]
    [Description("Set the directory to which persistent secrets are stored")]
    [DisplayName("SecretDirectory")]
    public string? SecretDirectory
    {
        get => _secretDirectory;
        set
        {
            _secretDirectory = value;
            SetValue("secret_directory", value);
        }
    }
    string? _secretDirectory;

    [Category("Configuration")]
    [Description("In which scenarios to use storage block prefetching")]
    [DisplayName("StorageBlockPrefetch")]
    public string? StorageBlockPrefetch
    {
        get => _storageBlockPrefetch;
        set
        {
            _storageBlockPrefetch = value;
            SetValue("storage_block_prefetch", value);
        }
    }
    string? _storageBlockPrefetch;

    [Category("Configuration")]
    [Description("Serialize on checkpoint with compatibility for a given duckdb version")]
    [DisplayName("StorageCompatibilityVersion")]
    public string? StorageCompatibilityVersion
    {
        get => _storageCompatibilityVersion;
        set
        {
            _storageCompatibilityVersion = value;
            SetValue("storage_compatibility_version", value);
        }
    }
    string? _storageCompatibilityVersion;

    [Category("Configuration")]
    [Description("The maximum memory to buffer between fetching from a streaming result (e.g. 1GB)")]
    [DisplayName("StreamingBufferSize")]
    public string? StreamingBufferSize
    {
        get => _streamingBufferSize;
        set
        {
            _streamingBufferSize = value;
            SetValue("streaming_buffer_size", value);
        }
    }
    string? _streamingBufferSize;

    [Category("Configuration")]
    [Description("Set the directory to which to write temp files")]
    [DisplayName("TempDirectory")]
    public string? TempDirectory
    {
        get => _tempDirectory;
        set
        {
            _tempDirectory = value;
            SetValue("temp_directory", value);
        }
    }
    string? _tempDirectory;

    [Category("Configuration")]
    [Description("Encrypt all temporary files if database is encrypted")]
    [DisplayName("TempFileEncryption")]
    public bool? TempFileEncryption
    {
        get => _tempFileEncryption;
        set
        {
            _tempFileEncryption = value;
            SetValue("temp_file_encryption", value);
        }
    }
    bool? _tempFileEncryption;

    [Category("Configuration")]
    [Description("The number of total threads used by the system.")]
    [DisplayName("Threads")]
    public ulong? Threads
    {
        get => _threads;
        set
        {
            _threads = value;
            SetValue("threads", value);
        }
    }
    ulong? _threads;

    [Category("Configuration")]
    [Description("Cache validation mode: VALIDATE_ALL (default, validate all cache entries), VALIDATE_REMOTE (validate only remote cache entries), or NO_VALIDATION (disable cache validation).")]
    [DisplayName("ValidateExternalFileCache")]
    public string? ValidateExternalFileCache
    {
        get => _validateExternalFileCache;
        set
        {
            _validateExternalFileCache = value;
            SetValue("validate_external_file_cache", value);
        }
    }
    string? _validateExternalFileCache;

    [Category("Configuration")]
    [Description("Minimum size of a rowgroup to enable VARIANT shredding, or set to -1 to disable entirely. Defaults to 1/4th of a rowgroup")]
    [DisplayName("VariantMinimumShreddingSize")]
    public ulong? VariantMinimumShreddingSize
    {
        get => _variantMinimumShreddingSize;
        set
        {
            _variantMinimumShreddingSize = value;
            SetValue("variant_minimum_shredding_size", value);
        }
    }
    ulong? _variantMinimumShreddingSize;

    [Category("Configuration")]
    [Description("The WAL size threshold at which to automatically trigger a checkpoint (e.g. 1GB)")]
    [DisplayName("WalAutocheckpoint")]
    public string? WalAutocheckpoint
    {
        get => _walAutocheckpoint;
        set
        {
            _walAutocheckpoint = value;
            SetValue("wal_autocheckpoint", value);
        }
    }
    string? _walAutocheckpoint;

    [Category("Configuration")]
    [Description("Trigger automatic checkpoint when WAL entry count reaches or exceeds N (0 = disabled)")]
    [DisplayName("WalAutocheckpointEntries")]
    public string? WalAutocheckpointEntries
    {
        get => _walAutocheckpointEntries;
        set
        {
            _walAutocheckpointEntries = value;
            SetValue("wal_autocheckpoint_entries", value);
        }
    }
    string? _walAutocheckpointEntries;

    [Category("Configuration")]
    [Description("Escalate all warnings to errors.")]
    [DisplayName("WarningsAsErrors")]
    public bool? WarningsAsErrors
    {
        get => _warningsAsErrors;
        set
        {
            _warningsAsErrors = value;
            SetValue("warnings_as_errors", value);
        }
    }
    bool? _warningsAsErrors;

    [Category("Configuration")]
    [Description("The number of total threads used by the system.")]
    [DisplayName("WorkerThreads")]
    public ulong? WorkerThreads
    {
        get => _workerThreads;
        set
        {
            _workerThreads = value;
            SetValue("worker_threads", value);
        }
    }
    ulong? _workerThreads;

    [Category("Configuration")]
    [Description("The amount of row groups to buffer in bulk ingestion prior to flushing them together. Reducing this setting can reduce memory consumption.")]
    [DisplayName("WriteBufferRowGroupCount")]
    public ulong? WriteBufferRowGroupCount
    {
        get => _writeBufferRowGroupCount;
        set
        {
            _writeBufferRowGroupCount = value;
            SetValue("write_buffer_row_group_count", value);
        }
    }
    ulong? _writeBufferRowGroupCount;

    [Category("Configuration")]
    [Description("The maximum data to buffer in row groups (in bytes) to buffer prior to flushing them together. When either this limit is reached, or write_buffer_row_group_count is reached, we flush the data to disk. Defaults to 20% of memory limit divided by thread count.")]
    [DisplayName("WriteBufferRowGroupMemoryLimit")]
    public string? WriteBufferRowGroupMemoryLimit
    {
        get => _writeBufferRowGroupMemoryLimit;
        set
        {
            _writeBufferRowGroupMemoryLimit = value;
            SetValue("write_buffer_row_group_memory_limit", value);
        }
    }
    string? _writeBufferRowGroupMemoryLimit;

    [Category("Configuration")]
    [Description("The (average) length at which to enable ZSTD compression, defaults to 4096")]
    [DisplayName("ZstdMinStringLength")]
    public ulong? ZstdMinStringLength
    {
        get => _zstdMinStringLength;
        set
        {
            _zstdMinStringLength = value;
            SetValue("zstd_min_string_length", value);
        }
    }
    ulong? _zstdMinStringLength;

	[Category("Debug")]
	[Description("DEBUG SETTING: force use of IEJoin to implement AsOf joins")]
	[DisplayName("DebugAsofIejoin")]
	public string? DebugAsofIejoin
	{
		get => _debugAsofIejoin;
		set
		{
			_debugAsofIejoin = value;
			SetValue("debug_asof_iejoin", value);
		}
	}
	string? _debugAsofIejoin;

	[Category("Debug")]
	[Description("DEBUG SETTING: trigger an abort while checkpointing for testing purposes")]
	[DisplayName("DebugCheckpointAbort")]
	public string? DebugCheckpointAbort
	{
		get => _debugCheckpointAbort;
		set
		{
			_debugCheckpointAbort = value;
			SetValue("debug_checkpoint_abort", value);
		}
	}
	string? _debugCheckpointAbort;

	[Category("Debug")]
	[Description("DEBUG SETTING: time to sleep before a checkpoint")]
	[DisplayName("DebugCheckpointSleepMs")]
	public string? DebugCheckpointSleepMs
	{
		get => _debugCheckpointSleepMs;
		set
		{
			_debugCheckpointSleepMs = value;
			SetValue("debug_checkpoint_sleep_ms", value);
		}
	}
	string? _debugCheckpointSleepMs;

	[Category("Debug")]
	[Description("DEBUG SETTING: time for the eviction queue to sleep before acquiring shared ownership of block memory")]
	[DisplayName("DebugEvictionQueueSleepMicroSeconds")]
	public string? DebugEvictionQueueSleepMicroSeconds
	{
		get => _debugEvictionQueueSleepMicroSeconds;
		set
		{
			_debugEvictionQueueSleepMicroSeconds = value;
			SetValue("debug_eviction_queue_sleep_micro_seconds", value);
		}
	}
	string? _debugEvictionQueueSleepMicroSeconds;

	[Category("Debug")]
	[Description("DEBUG SETTING: force out-of-core computation for operators that support it, used for testing")]
	[DisplayName("DebugForceExternal")]
	public string? DebugForceExternal
	{
		get => _debugForceExternal;
		set
		{
			_debugForceExternal = value;
			SetValue("debug_force_external", value);
		}
	}
	string? _debugForceExternal;

	[Category("Debug")]
	[Description("DEBUG SETTING: Force disable cross product generation when hyper graph isn't connected, used for testing")]
	[DisplayName("DebugForceNoCrossProduct")]
	public string? DebugForceNoCrossProduct
	{
		get => _debugForceNoCrossProduct;
		set
		{
			_debugForceNoCrossProduct = value;
			SetValue("debug_force_no_cross_product", value);
		}
	}
	string? _debugForceNoCrossProduct;

	[Category("Debug")]
	[Description("DEBUG SETTING: force use of given strategy for executing physical table scans")]
	[DisplayName("DebugPhysicalTableScanExecutionStrategy")]
	public string? DebugPhysicalTableScanExecutionStrategy
	{
		get => _debugPhysicalTableScanExecutionStrategy;
		set
		{
			_debugPhysicalTableScanExecutionStrategy = value;
			SetValue("debug_physical_table_scan_execution_strategy", value);
		}
	}
	string? _debugPhysicalTableScanExecutionStrategy;

	[Category("Debug")]
	[Description("DEBUG SETTING: skip checkpointing on commit")]
	[DisplayName("DebugSkipCheckpointOnCommit")]
	public string? DebugSkipCheckpointOnCommit
	{
		get => _debugSkipCheckpointOnCommit;
		set
		{
			_debugSkipCheckpointOnCommit = value;
			SetValue("debug_skip_checkpoint_on_commit", value);
		}
	}
	string? _debugSkipCheckpointOnCommit;

	[Category("Debug")]
	[Description("DEBUG SETTING: verify block metadata during checkpointing")]
	[DisplayName("DebugVerifyBlocks")]
	public string? DebugVerifyBlocks
	{
		get => _debugVerifyBlocks;
		set
		{
			_debugVerifyBlocks = value;
			SetValue("debug_verify_blocks", value);
		}
	}
	string? _debugVerifyBlocks;

	[Category("Debug")]
	[Description("DEBUG SETTING: enable vector verification")]
	[DisplayName("DebugVerifyVector")]
	public string? DebugVerifyVector
	{
		get => _debugVerifyVector;
		set
		{
			_debugVerifyVector = value;
			SetValue("debug_verify_vector", value);
		}
	}
	string? _debugVerifyVector;

	[Category("Debug")]
	[Description("DEBUG SETTING: switch window mode to use")]
	[DisplayName("DebugWindowMode")]
	public string? DebugWindowMode
	{
		get => _debugWindowMode;
		set
		{
			_debugWindowMode = value;
			SetValue("debug_window_mode", value);
		}
	}
	string? _debugWindowMode;

	[Category("Debug")]
	[Description("DEBUG SETTING: disable a specific set of optimizers (comma separated)")]
	[DisplayName("DisabledOptimizers")]
	public string? DisabledOptimizers
	{
		get => _disabledOptimizers;
		set
		{
			_disabledOptimizers = value;
			SetValue("disabled_optimizers", value);
		}
	}
	string? _disabledOptimizers;

	[Category("Debug")]
	[Description("DEBUG SETTING: forces a specific bitpacking mode")]
	[DisplayName("ForceBitpackingMode")]
	public string? ForceBitpackingMode
	{
		get => _forceBitpackingMode;
		set
		{
			_forceBitpackingMode = value;
			SetValue("force_bitpacking_mode", value);
		}
	}
	string? _forceBitpackingMode;

	[Category("Debug")]
	[Description("DEBUG SETTING: forces a specific compression method to be used")]
	[DisplayName("ForceCompression")]
	public string? ForceCompression
	{
		get => _forceCompression;
		set
		{
			_forceCompression = value;
			SetValue("force_compression", value);
		}
	}
	string? _forceCompression;

	[Category("Ducklake")]
    [DisplayName("DucklakeDefaultDataInliningRowLimit")]
    public string? DucklakeDefaultDataInliningRowLimit
    {
        get => _ducklakeDefaultDataInliningRowLimit;
        set
        {
            _ducklakeDefaultDataInliningRowLimit = value;
            SetValue("ducklake_default_data_inlining_row_limit", value);
        }
    }
    string? _ducklakeDefaultDataInliningRowLimit;

    [Category("Ducklake")]
    [DisplayName("DucklakeMaxRetryCount")]
    public string? DucklakeMaxRetryCount
    {
        get => _ducklakeMaxRetryCount;
        set
        {
            _ducklakeMaxRetryCount = value;
            SetValue("ducklake_max_retry_count", value);
        }
    }
    string? _ducklakeMaxRetryCount;

    [Category("Ducklake")]
    [DisplayName("DucklakeRetryBackoff")]
    public string? DucklakeRetryBackoff
    {
        get => _ducklakeRetryBackoff;
        set
        {
            _ducklakeRetryBackoff = value;
            SetValue("ducklake_retry_backoff", value);
        }
    }
    string? _ducklakeRetryBackoff;

    [Category("Ducklake")]
    [DisplayName("DucklakeRetryWaitMs")]
    public string? DucklakeRetryWaitMs
    {
        get => _ducklakeRetryWaitMs;
        set
        {
            _ducklakeRetryWaitMs = value;
            SetValue("ducklake_retry_wait_ms", value);
        }
    }
    string? _ducklakeRetryWaitMs;

    [Category("Ducklake")]
    [DisplayName("DucklakeTargetFileSize")]
    public string? DucklakeTargetFileSize
    {
        get => _ducklakeTargetFileSize;
        set
        {
            _ducklakeTargetFileSize = value;
            SetValue("ducklake_target_file_size", value);
        }
    }
    string? _ducklakeTargetFileSize;

    [Category("Ducklake")]
    [DisplayName("DucklakeWriteDeletionVectors")]
    public string? DucklakeWriteDeletionVectors
    {
        get => _ducklakeWriteDeletionVectors;
        set
        {
            _ducklakeWriteDeletionVectors = value;
            SetValue("ducklake_write_deletion_vectors", value);
        }
    }
    string? _ducklakeWriteDeletionVectors;

    [Category("Httpfs")]
    [Description("Allow '*' character in URLs users can query")]
    [DisplayName("AllowAsterisksInHttpPaths")]
    public bool? AllowAsterisksInHttpPaths
    {
        get => _allowAsterisksInHttpPaths;
        set
        {
            _allowAsterisksInHttpPaths = value;
            SetValue("allow_asterisks_in_http_paths", value);
        }
    }
    bool? _allowAsterisksInHttpPaths;

    [Category("Httpfs")]
    [Description("Allows automatically falling back to full file downloads when possible.")]
    [DisplayName("AutoFallbackToFullDownload")]
    public bool? AutoFallbackToFullDownload
    {
        get => _autoFallbackToFullDownload;
        set
        {
            _autoFallbackToFullDownload = value;
            SetValue("auto_fallback_to_full_download", value);
        }
    }
    bool? _autoFallbackToFullDownload;

    [Category("Httpfs")]
    [Description("Path to a custom certificate file for self-signed certificates.")]
    [DisplayName("CaCertFile")]
    public string? CaCertFile
    {
        get => _caCertFile;
        set
        {
            _caCertFile = value;
            SetValue("ca_cert_file", value);
        }
    }
    string? _caCertFile;

    [Category("Httpfs")]
    [Description("Enable server side certificate verification for CURL backend.")]
    [DisplayName("EnableCurlServerCertVerification")]
    public bool? EnableCurlServerCertVerification
    {
        get => _enableCurlServerCertVerification;
        set
        {
            _enableCurlServerCertVerification = value;
            SetValue("enable_curl_server_cert_verification", value);
        }
    }
    bool? _enableCurlServerCertVerification;

    [Category("Httpfs")]
    [Description("Automatically fetch AWS credentials from environment variables.")]
    [DisplayName("EnableGlobalS3Configuration")]
    public bool? EnableGlobalS3Configuration
    {
        get => _enableGlobalS3Configuration;
        set
        {
            _enableGlobalS3Configuration = value;
            SetValue("enable_global_s3_configuration", value);
        }
    }
    bool? _enableGlobalS3Configuration;

    [Category("Httpfs")]
    [Description("Enable server side certificate verification.")]
    [DisplayName("EnableServerCertVerification")]
    public bool? EnableServerCertVerification
    {
        get => _enableServerCertVerification;
        set
        {
            _enableServerCertVerification = value;
            SetValue("enable_server_cert_verification", value);
        }
    }
    bool? _enableServerCertVerification;

    [Category("Httpfs")]
    [Description("Forces upfront download of file")]
    [DisplayName("ForceDownload")]
    public bool? ForceDownload
    {
        get => _forceDownload;
        set
        {
            _forceDownload = value;
            SetValue("force_download", value);
        }
    }
    bool? _forceDownload;

    [Category("Httpfs")]
    [Description("Forces upfront download of files smaller than the given size in bytes")]
    [DisplayName("ForceDownloadThreshold")]
    public ulong? ForceDownloadThreshold
    {
        get => _forceDownloadThreshold;
        set
        {
            _forceDownloadThreshold = value;
            SetValue("force_download_threshold", value);
        }
    }
    ulong? _forceDownloadThreshold;

    [Category("Httpfs")]
    [DisplayName("HfMaxPerPage")]
    public string? HfMaxPerPage
    {
        get => _hfMaxPerPage;
        set
        {
            _hfMaxPerPage = value;
            SetValue("hf_max_per_page", value);
        }
    }
    string? _hfMaxPerPage;

    [Category("Httpfs")]
    [Description("Keep alive connections. Setting this to false can help when running into connection failures.")]
    [DisplayName("HttpKeepAlive")]
    public bool? HttpKeepAlive
    {
        get => _httpKeepAlive;
        set
        {
            _httpKeepAlive = value;
            SetValue("http_keep_alive", value);
        }
    }
    bool? _httpKeepAlive;

    [Category("Httpfs")]
    [Description("HTTP retries on I/O error")]
    [DisplayName("HttpRetries")]
    public ulong? HttpRetries
    {
        get => _httpRetries;
        set
        {
            _httpRetries = value;
            SetValue("http_retries", value);
        }
    }
    ulong? _httpRetries;

    [Category("Httpfs")]
    [Description("Backoff factor for exponentially increasing retry wait time")]
    [DisplayName("HttpRetryBackoff")]
    public float? HttpRetryBackoff
    {
        get => _httpRetryBackoff;
        set
        {
            _httpRetryBackoff = value;
            SetValue("http_retry_backoff", value);
        }
    }
    float? _httpRetryBackoff;

    [Category("Httpfs")]
    [Description("Time between retries")]
    [DisplayName("HttpRetryWaitMs")]
    public ulong? HttpRetryWaitMs
    {
        get => _httpRetryWaitMs;
        set
        {
            _httpRetryWaitMs = value;
            SetValue("http_retry_wait_ms", value);
        }
    }
    ulong? _httpRetryWaitMs;

    [Category("Httpfs")]
    [Description("HTTP timeout read/write/connection/retry (in seconds)")]
    [DisplayName("HttpTimeout")]
    public ulong? HttpTimeout
    {
        get => _httpTimeout;
        set
        {
            _httpTimeout = value;
            SetValue("http_timeout", value);
        }
    }
    ulong? _httpTimeout;

    [Category("Httpfs")]
    [Description("Select which is the HTTPUtil implementation to be used")]
    [DisplayName("HttpfsClientImplementation")]
    public string? HttpfsClientImplementation
    {
        get => _httpfsClientImplementation;
        set
        {
            _httpfsClientImplementation = value;
            SetValue("httpfs_client_implementation", value);
        }
    }
    string? _httpfsClientImplementation;

    [Category("Httpfs")]
    [Description("Enable connection caching for HTTP requests")]
    [DisplayName("HttpfsConnectionCaching")]
    public bool? HttpfsConnectionCaching
    {
        get => _httpfsConnectionCaching;
        set
        {
            _httpfsConnectionCaching = value;
            SetValue("httpfs_connection_caching", value);
        }
    }
    bool? _httpfsConnectionCaching;

    [Category("Httpfs")]
    [DisplayName("HttpfsEnableCredentialRefresh")]
    public string? HttpfsEnableCredentialRefresh
    {
        get => _httpfsEnableCredentialRefresh;
        set
        {
            _httpfsEnableCredentialRefresh = value;
            SetValue("httpfs_enable_credential_refresh", value);
        }
    }
    string? _httpfsEnableCredentialRefresh;

    [Category("Httpfs")]
    [Description("Merges http secret params into S3 requests")]
    [DisplayName("MergeHttpSecretIntoS3Request")]
    public bool? MergeHttpSecretIntoS3Request
    {
        get => _mergeHttpSecretIntoS3Request;
        set
        {
            _mergeHttpSecretIntoS3Request = value;
            SetValue("merge_http_secret_into_s3_request", value);
        }
    }
    bool? _mergeHttpSecretIntoS3Request;

    [Category("Httpfs")]
    [DisplayName("S3AccessKeyId")]
    public string? S3AccessKeyId
    {
        get => _s3AccessKeyId;
        set
        {
            _s3AccessKeyId = value;
            SetValue("s3_access_key_id", value);
        }
    }
    string? _s3AccessKeyId;

    [Category("Httpfs")]
    [Description("Whether globs on S3-like storage are optimized with recursive strategy (alterative is listing)")]
    [DisplayName("S3AllowRecursiveGlobbing")]
    public bool? S3AllowRecursiveGlobbing
    {
        get => _s3AllowRecursiveGlobbing;
        set
        {
            _s3AllowRecursiveGlobbing = value;
            SetValue("s3_allow_recursive_globbing", value);
        }
    }
    bool? _s3AllowRecursiveGlobbing;

    [Category("Httpfs")]
    [Description("S3 Endpoint")]
    [DisplayName("S3Endpoint")]
    public string? S3Endpoint
    {
        get => _s3Endpoint;
        set
        {
            _s3Endpoint = value;
            SetValue("s3_endpoint", value);
        }
    }
    string? _s3Endpoint;

    [Category("Httpfs")]
    [Description("S3 KMS Key ID")]
    [DisplayName("S3KmsKeyId")]
    public string? S3KmsKeyId
    {
        get => _s3KmsKeyId;
        set
        {
            _s3KmsKeyId = value;
            SetValue("s3_kms_key_id", value);
        }
    }
    string? _s3KmsKeyId;

    [Category("Httpfs")]
    [Description("S3 Region")]
    [DisplayName("S3Region")]
    public string? S3Region
    {
        get => _s3Region;
        set
        {
            _s3Region = value;
            SetValue("s3_region", value);
        }
    }
    string? _s3Region;

    [Category("Httpfs")]
    [Description("S3 use requester pays mode")]
    [DisplayName("S3RequesterPays")]
    public bool? S3RequesterPays
    {
        get => _s3RequesterPays;
        set
        {
            _s3RequesterPays = value;
            SetValue("s3_requester_pays", value);
        }
    }
    bool? _s3RequesterPays;

    [Category("Httpfs")]
    [Description("S3 Access Key")]
    [DisplayName("S3SecretAccessKey")]
    public string? S3SecretAccessKey
    {
        get => _s3SecretAccessKey;
        set
        {
            _s3SecretAccessKey = value;
            SetValue("s3_secret_access_key", value);
        }
    }
    string? _s3SecretAccessKey;

    [Category("Httpfs")]
    [Description("S3 Session Token")]
    [DisplayName("S3SessionToken")]
    public string? S3SessionToken
    {
        get => _s3SessionToken;
        set
        {
            _s3SessionToken = value;
            SetValue("s3_session_token", value);
        }
    }
    string? _s3SessionToken;

    [Category("Httpfs")]
    [Description("S3 Uploader max filesize (between 50GB and 5TB)")]
    [DisplayName("S3UploaderMaxFilesize")]
    public string? S3UploaderMaxFilesize
    {
        get => _s3UploaderMaxFilesize;
        set
        {
            _s3UploaderMaxFilesize = value;
            SetValue("s3_uploader_max_filesize", value);
        }
    }
    string? _s3UploaderMaxFilesize;

    [Category("Httpfs")]
    [Description("S3 Uploader max parts per file (between 1 and 10000)")]
    [DisplayName("S3UploaderMaxPartsPerFile")]
    public ulong? S3UploaderMaxPartsPerFile
    {
        get => _s3UploaderMaxPartsPerFile;
        set
        {
            _s3UploaderMaxPartsPerFile = value;
            SetValue("s3_uploader_max_parts_per_file", value);
        }
    }
    ulong? _s3UploaderMaxPartsPerFile;

    [Category("Httpfs")]
    [Description("S3 Uploader global thread limit")]
    [DisplayName("S3UploaderThreadLimit")]
    public ulong? S3UploaderThreadLimit
    {
        get => _s3UploaderThreadLimit;
        set
        {
            _s3UploaderThreadLimit = value;
            SetValue("s3_uploader_thread_limit", value);
        }
    }
    ulong? _s3UploaderThreadLimit;

    [Category("Httpfs")]
    [Description("Disable Globs and Query Parameters on S3 URLs")]
    [DisplayName("S3UrlCompatibilityMode")]
    public bool? S3UrlCompatibilityMode
    {
        get => _s3UrlCompatibilityMode;
        set
        {
            _s3UrlCompatibilityMode = value;
            SetValue("s3_url_compatibility_mode", value);
        }
    }
    bool? _s3UrlCompatibilityMode;

    [Category("Httpfs")]
    [Description("S3 URL style")]
    [DisplayName("S3UrlStyle")]
    public string? S3UrlStyle
    {
        get => _s3UrlStyle;
        set
        {
            _s3UrlStyle = value;
            SetValue("s3_url_style", value);
        }
    }
    string? _s3UrlStyle;

    [Category("Httpfs")]
    [Description("S3 use SSL")]
    [DisplayName("S3UseSsl")]
    public bool? S3UseSsl
    {
        get => _s3UseSsl;
        set
        {
            _s3UseSsl = value;
            SetValue("s3_use_ssl", value);
        }
    }
    bool? _s3UseSsl;

    [Category("Httpfs")]
    [Description("Pin S3 reads to a specific object version for consistency")]
    [DisplayName("S3VersionIdPinning")]
    public bool? S3VersionIdPinning
    {
        get => _s3VersionIdPinning;
        set
        {
            _s3VersionIdPinning = value;
            SetValue("s3_version_id_pinning", value);
        }
    }
    bool? _s3VersionIdPinning;

    [Category("Httpfs")]
    [Description("Disable checks on ETag consistency")]
    [DisplayName("UnsafeDisableEtagChecks")]
    public bool? UnsafeDisableEtagChecks
    {
        get => _unsafeDisableEtagChecks;
        set
        {
            _unsafeDisableEtagChecks = value;
            SetValue("unsafe_disable_etag_checks", value);
        }
    }
    bool? _unsafeDisableEtagChecks;

    [Category("Iceberg")]
    [DisplayName("IcebergLoggingPostBodyTruncateLimit")]
    public string? IcebergLoggingPostBodyTruncateLimit
    {
        get => _icebergLoggingPostBodyTruncateLimit;
        set
        {
            _icebergLoggingPostBodyTruncateLimit = value;
            SetValue("iceberg_logging_post_body_truncate_limit", value);
        }
    }
    string? _icebergLoggingPostBodyTruncateLimit;

    [Category("Iceberg")]
    [DisplayName("IcebergTestForceTokenExpiry")]
    public string? IcebergTestForceTokenExpiry
    {
        get => _icebergTestForceTokenExpiry;
        set
        {
            _icebergTestForceTokenExpiry = value;
            SetValue("iceberg_test_force_token_expiry", value);
        }
    }
    string? _icebergTestForceTokenExpiry;

    [Category("Iceberg")]
    [DisplayName("IcebergUseMetadataLog")]
    public string? IcebergUseMetadataLog
    {
        get => _icebergUseMetadataLog;
        set
        {
            _icebergUseMetadataLog = value;
            SetValue("iceberg_use_metadata_log", value);
        }
    }
    string? _icebergUseMetadataLog;

    [Category("Iceberg")]
    [DisplayName("IcebergViaAwsSdkForCatalogInteractions")]
    public string? IcebergViaAwsSdkForCatalogInteractions
    {
        get => _icebergViaAwsSdkForCatalogInteractions;
        set
        {
            _icebergViaAwsSdkForCatalogInteractions = value;
            SetValue("iceberg_via_aws_sdk_for_catalog_interactions", value);
        }
    }
    string? _icebergViaAwsSdkForCatalogInteractions;

    [Category("Iceberg")]
    [DisplayName("IgnoreRowGroupSizeForPartitionedTables")]
    public string? IgnoreRowGroupSizeForPartitionedTables
    {
        get => _ignoreRowGroupSizeForPartitionedTables;
        set
        {
            _ignoreRowGroupSizeForPartitionedTables = value;
            SetValue("ignore_row_group_size_for_partitioned_tables", value);
        }
    }
    string? _ignoreRowGroupSizeForPartitionedTables;

    [Category("Iceberg")]
    [DisplayName("IgnoreTargetFileSizeForPartitionedTables")]
    public string? IgnoreTargetFileSizeForPartitionedTables
    {
        get => _ignoreTargetFileSizeForPartitionedTables;
        set
        {
            _ignoreTargetFileSizeForPartitionedTables = value;
            SetValue("ignore_target_file_size_for_partitioned_tables", value);
        }
    }
    string? _ignoreTargetFileSizeForPartitionedTables;

    [Category("Iceberg")]
    [DisplayName("UnsafeEnableVersionGuessing")]
    public string? UnsafeEnableVersionGuessing
    {
        get => _unsafeEnableVersionGuessing;
        set
        {
            _unsafeEnableVersionGuessing = value;
            SetValue("unsafe_enable_version_guessing", value);
        }
    }
    string? _unsafeEnableVersionGuessing;

    [Category("Iceberg")]
    [DisplayName("UnsafeIcebergIgnoreSortOrder")]
    public string? UnsafeIcebergIgnoreSortOrder
    {
        get => _unsafeIcebergIgnoreSortOrder;
        set
        {
            _unsafeIcebergIgnoreSortOrder = value;
            SetValue("unsafe_iceberg_ignore_sort_order", value);
        }
    }
    string? _unsafeIcebergIgnoreSortOrder;

    [Category("Icu")]
    [Description("The current calendar")]
    [DisplayName("Calendar")]
    public string? Calendar
    {
        get => _calendar;
        set
        {
            _calendar = value;
            SetValue("calendar", value);
        }
    }
    string? _calendar;

    [Category("Icu")]
    [Description("The current timezone")]
    [DisplayName("Timezone")]
    public string? Timezone
    {
        get => _timezone;
        set
        {
            _timezone = value;
            SetValue("timezone", value);
        }
    }
    string? _timezone;

    [Category("MysqlScanner")]
    [DisplayName("MysqlAdaptiveReplanEnabled")]
    public string? MysqlAdaptiveReplanEnabled
    {
        get => _mysqlAdaptiveReplanEnabled;
        set
        {
            _mysqlAdaptiveReplanEnabled = value;
            SetValue("mysql_adaptive_replan_enabled", value);
        }
    }
    string? _mysqlAdaptiveReplanEnabled;

    [Category("MysqlScanner")]
    [DisplayName("MysqlAggregatePushdownEnabled")]
    public string? MysqlAggregatePushdownEnabled
    {
        get => _mysqlAggregatePushdownEnabled;
        set
        {
            _mysqlAggregatePushdownEnabled = value;
            SetValue("mysql_aggregate_pushdown_enabled", value);
        }
    }
    string? _mysqlAggregatePushdownEnabled;

    [Category("MysqlScanner")]
    [DisplayName("MysqlAllowResultsStreaming")]
    public string? MysqlAllowResultsStreaming
    {
        get => _mysqlAllowResultsStreaming;
        set
        {
            _mysqlAllowResultsStreaming = value;
            SetValue("mysql_allow_results_streaming", value);
        }
    }
    string? _mysqlAllowResultsStreaming;

    [Category("MysqlScanner")]
    [DisplayName("MysqlBit1AsBoolean")]
    public string? MysqlBit1AsBoolean
    {
        get => _mysqlBit1AsBoolean;
        set
        {
            _mysqlBit1AsBoolean = value;
            SetValue("mysql_bit1_as_boolean", value);
        }
    }
    string? _mysqlBit1AsBoolean;

    [Category("MysqlScanner")]
    [DisplayName("MysqlCompressionAwareCosts")]
    public string? MysqlCompressionAwareCosts
    {
        get => _mysqlCompressionAwareCosts;
        set
        {
            _mysqlCompressionAwareCosts = value;
            SetValue("mysql_compression_aware_costs", value);
        }
    }
    string? _mysqlCompressionAwareCosts;

    [Category("MysqlScanner")]
    [DisplayName("MysqlCompressionRatio")]
    public string? MysqlCompressionRatio
    {
        get => _mysqlCompressionRatio;
        set
        {
            _mysqlCompressionRatio = value;
            SetValue("mysql_compression_ratio", value);
        }
    }
    string? _mysqlCompressionRatio;

    [Category("MysqlScanner")]
    [DisplayName("MysqlDebugShowQueries")]
    public string? MysqlDebugShowQueries
    {
        get => _mysqlDebugShowQueries;
        set
        {
            _mysqlDebugShowQueries = value;
            SetValue("mysql_debug_show_queries", value);
        }
    }
    string? _mysqlDebugShowQueries;

    [Category("MysqlScanner")]
    [DisplayName("MysqlEnablePredicateAnalyzer")]
    public string? MysqlEnablePredicateAnalyzer
    {
        get => _mysqlEnablePredicateAnalyzer;
        set
        {
            _mysqlEnablePredicateAnalyzer = value;
            SetValue("mysql_enable_predicate_analyzer", value);
        }
    }
    string? _mysqlEnablePredicateAnalyzer;

    [Category("MysqlScanner")]
    [DisplayName("MysqlEnableTransactions")]
    public string? MysqlEnableTransactions
    {
        get => _mysqlEnableTransactions;
        set
        {
            _mysqlEnableTransactions = value;
            SetValue("mysql_enable_transactions", value);
        }
    }
    string? _mysqlEnableTransactions;

    [Category("MysqlScanner")]
    [DisplayName("MysqlExperimentalFilterPushdown")]
    public string? MysqlExperimentalFilterPushdown
    {
        get => _mysqlExperimentalFilterPushdown;
        set
        {
            _mysqlExperimentalFilterPushdown = value;
            SetValue("mysql_experimental_filter_pushdown", value);
        }
    }
    string? _mysqlExperimentalFilterPushdown;

    [Category("MysqlScanner")]
    [DisplayName("MysqlExplainValidationEnabled")]
    public string? MysqlExplainValidationEnabled
    {
        get => _mysqlExplainValidationEnabled;
        set
        {
            _mysqlExplainValidationEnabled = value;
            SetValue("mysql_explain_validation_enabled", value);
        }
    }
    string? _mysqlExplainValidationEnabled;

    [Category("MysqlScanner")]
    [DisplayName("MysqlHintInjectionEnabled")]
    public string? MysqlHintInjectionEnabled
    {
        get => _mysqlHintInjectionEnabled;
        set
        {
            _mysqlHintInjectionEnabled = value;
            SetValue("mysql_hint_injection_enabled", value);
        }
    }
    string? _mysqlHintInjectionEnabled;

    [Category("MysqlScanner")]
    [DisplayName("MysqlHintStalenessThreshold")]
    public string? MysqlHintStalenessThreshold
    {
        get => _mysqlHintStalenessThreshold;
        set
        {
            _mysqlHintStalenessThreshold = value;
            SetValue("mysql_hint_staleness_threshold", value);
        }
    }
    string? _mysqlHintStalenessThreshold;

    [Category("MysqlScanner")]
    [DisplayName("MysqlIncompleteDatesAsNulls")]
    public string? MysqlIncompleteDatesAsNulls
    {
        get => _mysqlIncompleteDatesAsNulls;
        set
        {
            _mysqlIncompleteDatesAsNulls = value;
            SetValue("mysql_incomplete_dates_as_nulls", value);
        }
    }
    string? _mysqlIncompleteDatesAsNulls;

    [Category("MysqlScanner")]
    [DisplayName("MysqlOrderPushdownEnabled")]
    public string? MysqlOrderPushdownEnabled
    {
        get => _mysqlOrderPushdownEnabled;
        set
        {
            _mysqlOrderPushdownEnabled = value;
            SetValue("mysql_order_pushdown_enabled", value);
        }
    }
    string? _mysqlOrderPushdownEnabled;

    [Category("MysqlScanner")]
    [DisplayName("MysqlPoolAcquireMode")]
    public string? MysqlPoolAcquireMode
    {
        get => _mysqlPoolAcquireMode;
        set
        {
            _mysqlPoolAcquireMode = value;
            SetValue("mysql_pool_acquire_mode", value);
        }
    }
    string? _mysqlPoolAcquireMode;

    [Category("MysqlScanner")]
    [DisplayName("MysqlPoolConnectionIdleTimeoutMillis")]
    public string? MysqlPoolConnectionIdleTimeoutMillis
    {
        get => _mysqlPoolConnectionIdleTimeoutMillis;
        set
        {
            _mysqlPoolConnectionIdleTimeoutMillis = value;
            SetValue("mysql_pool_connection_idle_timeout_millis", value);
        }
    }
    string? _mysqlPoolConnectionIdleTimeoutMillis;

    [Category("MysqlScanner")]
    [DisplayName("MysqlPoolConnectionMaxLifetimeMillis")]
    public string? MysqlPoolConnectionMaxLifetimeMillis
    {
        get => _mysqlPoolConnectionMaxLifetimeMillis;
        set
        {
            _mysqlPoolConnectionMaxLifetimeMillis = value;
            SetValue("mysql_pool_connection_max_lifetime_millis", value);
        }
    }
    string? _mysqlPoolConnectionMaxLifetimeMillis;

    [Category("MysqlScanner")]
    [DisplayName("MysqlPoolEnableReaperThread")]
    public string? MysqlPoolEnableReaperThread
    {
        get => _mysqlPoolEnableReaperThread;
        set
        {
            _mysqlPoolEnableReaperThread = value;
            SetValue("mysql_pool_enable_reaper_thread", value);
        }
    }
    string? _mysqlPoolEnableReaperThread;

    [Category("MysqlScanner")]
    [DisplayName("MysqlPoolEnableThreadLocalCache")]
    public string? MysqlPoolEnableThreadLocalCache
    {
        get => _mysqlPoolEnableThreadLocalCache;
        set
        {
            _mysqlPoolEnableThreadLocalCache = value;
            SetValue("mysql_pool_enable_thread_local_cache", value);
        }
    }
    string? _mysqlPoolEnableThreadLocalCache;

    [Category("MysqlScanner")]
    [DisplayName("MysqlPoolSize")]
    public string? MysqlPoolSize
    {
        get => _mysqlPoolSize;
        set
        {
            _mysqlPoolSize = value;
            SetValue("mysql_pool_size", value);
        }
    }
    string? _mysqlPoolSize;

    [Category("MysqlScanner")]
    [DisplayName("MysqlPoolWaitTimeoutMillis")]
    public string? MysqlPoolWaitTimeoutMillis
    {
        get => _mysqlPoolWaitTimeoutMillis;
        set
        {
            _mysqlPoolWaitTimeoutMillis = value;
            SetValue("mysql_pool_wait_timeout_millis", value);
        }
    }
    string? _mysqlPoolWaitTimeoutMillis;

    [Category("MysqlScanner")]
    [DisplayName("MysqlPushThresholdNoIndex")]
    public string? MysqlPushThresholdNoIndex
    {
        get => _mysqlPushThresholdNoIndex;
        set
        {
            _mysqlPushThresholdNoIndex = value;
            SetValue("mysql_push_threshold_no_index", value);
        }
    }
    string? _mysqlPushThresholdNoIndex;

    [Category("MysqlScanner")]
    [DisplayName("MysqlPushThresholdWithIndex")]
    public string? MysqlPushThresholdWithIndex
    {
        get => _mysqlPushThresholdWithIndex;
        set
        {
            _mysqlPushThresholdWithIndex = value;
            SetValue("mysql_push_threshold_with_index", value);
        }
    }
    string? _mysqlPushThresholdWithIndex;

    [Category("MysqlScanner")]
    [DisplayName("MysqlQueryTimeoutEnabled")]
    public string? MysqlQueryTimeoutEnabled
    {
        get => _mysqlQueryTimeoutEnabled;
        set
        {
            _mysqlQueryTimeoutEnabled = value;
            SetValue("mysql_query_timeout_enabled", value);
        }
    }
    string? _mysqlQueryTimeoutEnabled;

    [Category("MysqlScanner")]
    [DisplayName("MysqlQueryTimeoutMaxMs")]
    public string? MysqlQueryTimeoutMaxMs
    {
        get => _mysqlQueryTimeoutMaxMs;
        set
        {
            _mysqlQueryTimeoutMaxMs = value;
            SetValue("mysql_query_timeout_max_ms", value);
        }
    }
    string? _mysqlQueryTimeoutMaxMs;

    [Category("MysqlScanner")]
    [DisplayName("MysqlQueryTimeoutMinMs")]
    public string? MysqlQueryTimeoutMinMs
    {
        get => _mysqlQueryTimeoutMinMs;
        set
        {
            _mysqlQueryTimeoutMinMs = value;
            SetValue("mysql_query_timeout_min_ms", value);
        }
    }
    string? _mysqlQueryTimeoutMinMs;

    [Category("MysqlScanner")]
    [DisplayName("MysqlSessionTimeZone")]
    public string? MysqlSessionTimeZone
    {
        get => _mysqlSessionTimeZone;
        set
        {
            _mysqlSessionTimeZone = value;
            SetValue("mysql_session_time_zone", value);
        }
    }
    string? _mysqlSessionTimeZone;

    [Category("MysqlScanner")]
    [DisplayName("MysqlSqlBufferResult")]
    public string? MysqlSqlBufferResult
    {
        get => _mysqlSqlBufferResult;
        set
        {
            _mysqlSqlBufferResult = value;
            SetValue("mysql_sql_buffer_result", value);
        }
    }
    string? _mysqlSqlBufferResult;

    [Category("MysqlScanner")]
    [DisplayName("MysqlTimeAsTime")]
    public string? MysqlTimeAsTime
    {
        get => _mysqlTimeAsTime;
        set
        {
            _mysqlTimeAsTime = value;
            SetValue("mysql_time_as_time", value);
        }
    }
    string? _mysqlTimeAsTime;

    [Category("MysqlScanner")]
    [DisplayName("MysqlTinyint1AsBoolean")]
    public string? MysqlTinyint1AsBoolean
    {
        get => _mysqlTinyint1AsBoolean;
        set
        {
            _mysqlTinyint1AsBoolean = value;
            SetValue("mysql_tinyint1_as_boolean", value);
        }
    }
    string? _mysqlTinyint1AsBoolean;

    [Category("Parquet")]
    [Description("Enables the Parquet reader to identify a Variant structurally.")]
    [DisplayName("DeltaOnlyVariantEncodingEnabled")]
    public bool? DeltaOnlyVariantEncodingEnabled
    {
        get => _deltaOnlyVariantEncodingEnabled;
        set
        {
            _deltaOnlyVariantEncodingEnabled = value;
            SetValue("__delta_only_variant_encoding_enabled", value);
        }
    }
    bool? _deltaOnlyVariantEncodingEnabled;

    [Category("Parquet")]
    [Description("In Parquet files, interpret binary data as a string.")]
    [DisplayName("BinaryAsString")]
    public bool? BinaryAsString
    {
        get => _binaryAsString;
        set
        {
            _binaryAsString = value;
            SetValue("binary_as_string", value);
        }
    }
    bool? _binaryAsString;

    [Category("Parquet")]
    [Description("Disable the prefetching mechanism in Parquet")]
    [DisplayName("DisableParquetPrefetching")]
    public bool? DisableParquetPrefetching
    {
        get => _disableParquetPrefetching;
        set
        {
            _disableParquetPrefetching = value;
            SetValue("disable_parquet_prefetching", value);
        }
    }
    bool? _disableParquetPrefetching;

    [Category("Parquet")]
    [Description("Attempt to decode/encode geometry data in/as GeoParquet files if the spatial extension is present.")]
    [DisplayName("EnableGeoparquetConversion")]
    public bool? EnableGeoparquetConversion
    {
        get => _enableGeoparquetConversion;
        set
        {
            _enableGeoparquetConversion = value;
            SetValue("enable_geoparquet_conversion", value);
        }
    }
    bool? _enableGeoparquetConversion;

    [Category("Parquet")]
    [Description("Cache Parquet metadata - useful when reading the same files multiple times")]
    [DisplayName("ParquetMetadataCache")]
    public bool? ParquetMetadataCache
    {
        get => _parquetMetadataCache;
        set
        {
            _parquetMetadataCache = value;
            SetValue("parquet_metadata_cache", value);
        }
    }
    bool? _parquetMetadataCache;

    [Category("Parquet")]
    [Description("Use the prefetching mechanism for all types of parquet files")]
    [DisplayName("PrefetchAllParquetFiles")]
    public bool? PrefetchAllParquetFiles
    {
        get => _prefetchAllParquetFiles;
        set
        {
            _prefetchAllParquetFiles = value;
            SetValue("prefetch_all_parquet_files", value);
        }
    }
    bool? _prefetchAllParquetFiles;

    [Category("PostgresScanner")]
    [DisplayName("PgArrayAsVarchar")]
    public string? PgArrayAsVarchar
    {
        get => _pgArrayAsVarchar;
        set
        {
            _pgArrayAsVarchar = value;
            SetValue("pg_array_as_varchar", value);
        }
    }
    string? _pgArrayAsVarchar;

    [Category("PostgresScanner")]
    [DisplayName("PgConnectionCache")]
    public string? PgConnectionCache
    {
        get => _pgConnectionCache;
        set
        {
            _pgConnectionCache = value;
            SetValue("pg_connection_cache", value);
        }
    }
    string? _pgConnectionCache;

    [Category("PostgresScanner")]
    [DisplayName("PgConnectionLimit")]
    public string? PgConnectionLimit
    {
        get => _pgConnectionLimit;
        set
        {
            _pgConnectionLimit = value;
            SetValue("pg_connection_limit", value);
        }
    }
    string? _pgConnectionLimit;

    [Category("PostgresScanner")]
    [DisplayName("PgDebugShowQueries")]
    public string? PgDebugShowQueries
    {
        get => _pgDebugShowQueries;
        set
        {
            _pgDebugShowQueries = value;
            SetValue("pg_debug_show_queries", value);
        }
    }
    string? _pgDebugShowQueries;

    [Category("PostgresScanner")]
    [DisplayName("PgExperimentalFilterPushdown")]
    public string? PgExperimentalFilterPushdown
    {
        get => _pgExperimentalFilterPushdown;
        set
        {
            _pgExperimentalFilterPushdown = value;
            SetValue("pg_experimental_filter_pushdown", value);
        }
    }
    string? _pgExperimentalFilterPushdown;

    [Category("PostgresScanner")]
    [DisplayName("PgIdleInTransactionTimeoutMillis")]
    public string? PgIdleInTransactionTimeoutMillis
    {
        get => _pgIdleInTransactionTimeoutMillis;
        set
        {
            _pgIdleInTransactionTimeoutMillis = value;
            SetValue("pg_idle_in_transaction_timeout_millis", value);
        }
    }
    string? _pgIdleInTransactionTimeoutMillis;

    [Category("PostgresScanner")]
    [DisplayName("PgNullByteReplacement")]
    public string? PgNullByteReplacement
    {
        get => _pgNullByteReplacement;
        set
        {
            _pgNullByteReplacement = value;
            SetValue("pg_null_byte_replacement", value);
        }
    }
    string? _pgNullByteReplacement;

    [Category("PostgresScanner")]
    [DisplayName("PgOauthToken")]
    public string? PgOauthToken
    {
        get => _pgOauthToken;
        set
        {
            _pgOauthToken = value;
            SetValue("pg_oauth_token", value);
        }
    }
    string? _pgOauthToken;

    [Category("PostgresScanner")]
    [DisplayName("PgPagesPerTask")]
    public string? PgPagesPerTask
    {
        get => _pgPagesPerTask;
        set
        {
            _pgPagesPerTask = value;
            SetValue("pg_pages_per_task", value);
        }
    }
    string? _pgPagesPerTask;

    [Category("PostgresScanner")]
    [DisplayName("PgPoolAcquireMode")]
    public string? PgPoolAcquireMode
    {
        get => _pgPoolAcquireMode;
        set
        {
            _pgPoolAcquireMode = value;
            SetValue("pg_pool_acquire_mode", value);
        }
    }
    string? _pgPoolAcquireMode;

    [Category("PostgresScanner")]
    [DisplayName("PgPoolEnableReaperThread")]
    public string? PgPoolEnableReaperThread
    {
        get => _pgPoolEnableReaperThread;
        set
        {
            _pgPoolEnableReaperThread = value;
            SetValue("pg_pool_enable_reaper_thread", value);
        }
    }
    string? _pgPoolEnableReaperThread;

    [Category("PostgresScanner")]
    [DisplayName("PgPoolEnableThreadLocalCache")]
    public string? PgPoolEnableThreadLocalCache
    {
        get => _pgPoolEnableThreadLocalCache;
        set
        {
            _pgPoolEnableThreadLocalCache = value;
            SetValue("pg_pool_enable_thread_local_cache", value);
        }
    }
    string? _pgPoolEnableThreadLocalCache;

    [Category("PostgresScanner")]
    [DisplayName("PgPoolHealthCheckQuery")]
    public string? PgPoolHealthCheckQuery
    {
        get => _pgPoolHealthCheckQuery;
        set
        {
            _pgPoolHealthCheckQuery = value;
            SetValue("pg_pool_health_check_query", value);
        }
    }
    string? _pgPoolHealthCheckQuery;

    [Category("PostgresScanner")]
    [DisplayName("PgPoolIdleTimeoutMillis")]
    public string? PgPoolIdleTimeoutMillis
    {
        get => _pgPoolIdleTimeoutMillis;
        set
        {
            _pgPoolIdleTimeoutMillis = value;
            SetValue("pg_pool_idle_timeout_millis", value);
        }
    }
    string? _pgPoolIdleTimeoutMillis;

    [Category("PostgresScanner")]
    [DisplayName("PgPoolMaxConnections")]
    public string? PgPoolMaxConnections
    {
        get => _pgPoolMaxConnections;
        set
        {
            _pgPoolMaxConnections = value;
            SetValue("pg_pool_max_connections", value);
        }
    }
    string? _pgPoolMaxConnections;

    [Category("PostgresScanner")]
    [DisplayName("PgPoolMaxLifetimeMillis")]
    public string? PgPoolMaxLifetimeMillis
    {
        get => _pgPoolMaxLifetimeMillis;
        set
        {
            _pgPoolMaxLifetimeMillis = value;
            SetValue("pg_pool_max_lifetime_millis", value);
        }
    }
    string? _pgPoolMaxLifetimeMillis;

    [Category("PostgresScanner")]
    [DisplayName("PgPoolWaitTimeoutMillis")]
    public string? PgPoolWaitTimeoutMillis
    {
        get => _pgPoolWaitTimeoutMillis;
        set
        {
            _pgPoolWaitTimeoutMillis = value;
            SetValue("pg_pool_wait_timeout_millis", value);
        }
    }
    string? _pgPoolWaitTimeoutMillis;

    [Category("PostgresScanner")]
    [DisplayName("PgStalenessQuery")]
    public string? PgStalenessQuery
    {
        get => _pgStalenessQuery;
        set
        {
            _pgStalenessQuery = value;
            SetValue("pg_staleness_query", value);
        }
    }
    string? _pgStalenessQuery;

    [Category("PostgresScanner")]
    [DisplayName("PgStalenessQueryEnabled")]
    public string? PgStalenessQueryEnabled
    {
        get => _pgStalenessQueryEnabled;
        set
        {
            _pgStalenessQueryEnabled = value;
            SetValue("pg_staleness_query_enabled", value);
        }
    }
    string? _pgStalenessQueryEnabled;

    [Category("PostgresScanner")]
    [DisplayName("PgStatementTimeoutMillis")]
    public string? PgStatementTimeoutMillis
    {
        get => _pgStatementTimeoutMillis;
        set
        {
            _pgStatementTimeoutMillis = value;
            SetValue("pg_statement_timeout_millis", value);
        }
    }
    string? _pgStatementTimeoutMillis;

    [Category("PostgresScanner")]
    [DisplayName("PgUseBinaryCopy")]
    public string? PgUseBinaryCopy
    {
        get => _pgUseBinaryCopy;
        set
        {
            _pgUseBinaryCopy = value;
            SetValue("pg_use_binary_copy", value);
        }
    }
    string? _pgUseBinaryCopy;

    [Category("PostgresScanner")]
    [DisplayName("PgUseCtidScan")]
    public string? PgUseCtidScan
    {
        get => _pgUseCtidScan;
        set
        {
            _pgUseCtidScan = value;
            SetValue("pg_use_ctid_scan", value);
        }
    }
    string? _pgUseCtidScan;

    [Category("PostgresScanner")]
    [DisplayName("PgUseInformationSchemaIntrospection")]
    public string? PgUseInformationSchemaIntrospection
    {
        get => _pgUseInformationSchemaIntrospection;
        set
        {
            _pgUseInformationSchemaIntrospection = value;
            SetValue("pg_use_information_schema_introspection", value);
        }
    }
    string? _pgUseInformationSchemaIntrospection;

    [Category("PostgresScanner")]
    [DisplayName("PgUseTextProtocol")]
    public string? PgUseTextProtocol
    {
        get => _pgUseTextProtocol;
        set
        {
            _pgUseTextProtocol = value;
            SetValue("pg_use_text_protocol", value);
        }
    }
    string? _pgUseTextProtocol;

    [Category("Quack")]
    [DisplayName("QuackAuthenticationFunction")]
    public string? QuackAuthenticationFunction
    {
        get => _quackAuthenticationFunction;
        set
        {
            _quackAuthenticationFunction = value;
            SetValue("quack_authentication_function", value);
        }
    }
    string? _quackAuthenticationFunction;

    [Category("Quack")]
    [DisplayName("QuackAuthorizationFunction")]
    public string? QuackAuthorizationFunction
    {
        get => _quackAuthorizationFunction;
        set
        {
            _quackAuthorizationFunction = value;
            SetValue("quack_authorization_function", value);
        }
    }
    string? _quackAuthorizationFunction;

    [Category("Quack")]
    [DisplayName("QuackFetchBatchChunks")]
    public string? QuackFetchBatchChunks
    {
        get => _quackFetchBatchChunks;
        set
        {
            _quackFetchBatchChunks = value;
            SetValue("quack_fetch_batch_chunks", value);
        }
    }
    string? _quackFetchBatchChunks;

    [Category("Quack")]
    [DisplayName("QuackLoadedAtUs")]
    public string? QuackLoadedAtUs
    {
        get => _quackLoadedAtUs;
        set
        {
            _quackLoadedAtUs = value;
            SetValue("quack_loaded_at_us", value);
        }
    }
    string? _quackLoadedAtUs;

    [Category("Quack")]
    [DisplayName("WhoamiHostname")]
    public string? WhoamiHostname
    {
        get => _whoamiHostname;
        set
        {
            _whoamiHostname = value;
            SetValue("whoami_hostname", value);
        }
    }
    string? _whoamiHostname;

    [Category("Quack")]
    [DisplayName("WhoamiMeta")]
    public string? WhoamiMeta
    {
        get => _whoamiMeta;
        set
        {
            _whoamiMeta = value;
            SetValue("whoami_meta", value);
        }
    }
    string? _whoamiMeta;

    [Category("Quack")]
    [DisplayName("WhoamiName")]
    public string? WhoamiName
    {
        get => _whoamiName;
        set
        {
            _whoamiName = value;
            SetValue("whoami_name", value);
        }
    }
    string? _whoamiName;

    [Category("Quack")]
    [DisplayName("WhoamiProvider")]
    public string? WhoamiProvider
    {
        get => _whoamiProvider;
        set
        {
            _whoamiProvider = value;
            SetValue("whoami_provider", value);
        }
    }
    string? _whoamiProvider;

    [Category("Quack")]
    [DisplayName("WhoamiRegion")]
    public string? WhoamiRegion
    {
        get => _whoamiRegion;
        set
        {
            _whoamiRegion = value;
            SetValue("whoami_region", value);
        }
    }
    string? _whoamiRegion;

    [Category("Quack")]
    [DisplayName("WhoamiStartedAt")]
    public string? WhoamiStartedAt
    {
        get => _whoamiStartedAt;
        set
        {
            _whoamiStartedAt = value;
            SetValue("whoami_started_at", value);
        }
    }
    string? _whoamiStartedAt;

    [Category("Spatial")]
    [DisplayName("GeometryAlwaysXy")]
    public string? GeometryAlwaysXy
    {
        get => _geometryAlwaysXy;
        set
        {
            _geometryAlwaysXy = value;
            SetValue("geometry_always_xy", value);
        }
    }
    string? _geometryAlwaysXy;

    [Category("Spatial")]
    [DisplayName("RtreeIndexScanMinRows")]
    public string? RtreeIndexScanMinRows
    {
        get => _rtreeIndexScanMinRows;
        set
        {
            _rtreeIndexScanMinRows = value;
            SetValue("rtree_index_scan_min_rows", value);
        }
    }
    string? _rtreeIndexScanMinRows;

    [Category("Spatial")]
    [DisplayName("RtreeIndexScanRatio")]
    public string? RtreeIndexScanRatio
    {
        get => _rtreeIndexScanRatio;
        set
        {
            _rtreeIndexScanRatio = value;
            SetValue("rtree_index_scan_ratio", value);
        }
    }
    string? _rtreeIndexScanRatio;

    [Category("SqliteScanner")]
    [DisplayName("SqliteAllVarchar")]
    public string? SqliteAllVarchar
    {
        get => _sqliteAllVarchar;
        set
        {
            _sqliteAllVarchar = value;
            SetValue("sqlite_all_varchar", value);
        }
    }
    string? _sqliteAllVarchar;

    [Category("SqliteScanner")]
    [DisplayName("SqliteDebugShowQueries")]
    public string? SqliteDebugShowQueries
    {
        get => _sqliteDebugShowQueries;
        set
        {
            _sqliteDebugShowQueries = value;
            SetValue("sqlite_debug_show_queries", value);
        }
    }
    string? _sqliteDebugShowQueries;

    [Category("SqliteScanner")]
    [DisplayName("SqliteDisableMultithreadedScans")]
    public string? SqliteDisableMultithreadedScans
    {
        get => _sqliteDisableMultithreadedScans;
        set
        {
            _sqliteDisableMultithreadedScans = value;
            SetValue("sqlite_disable_multithreaded_scans", value);
        }
    }
    string? _sqliteDisableMultithreadedScans;

    [Category("Ui")]
    [DisplayName("UiLocalPort")]
    public string? UiLocalPort
    {
        get => _uiLocalPort;
        set
        {
            _uiLocalPort = value;
            SetValue("ui_local_port", value);
        }
    }
    string? _uiLocalPort;

    [Category("Ui")]
    [DisplayName("UiPollingInterval")]
    public string? UiPollingInterval
    {
        get => _uiPollingInterval;
        set
        {
            _uiPollingInterval = value;
            SetValue("ui_polling_interval", value);
        }
    }
    string? _uiPollingInterval;

    [Category("Ui")]
    [DisplayName("UiRemoteUrl")]
    public string? UiRemoteUrl
    {
        get => _uiRemoteUrl;
        set
        {
            _uiRemoteUrl = value;
            SetValue("ui_remote_url", value);
        }
    }
    string? _uiRemoteUrl;

    [Category("Vss")]
    [DisplayName("HnswEfSearch")]
    public string? HnswEfSearch
    {
        get => _hnswEfSearch;
        set
        {
            _hnswEfSearch = value;
            SetValue("hnsw_ef_search", value);
        }
    }
    string? _hnswEfSearch;

    [Category("Vss")]
    [DisplayName("HnswEnableExperimentalPersistence")]
    public string? HnswEnableExperimentalPersistence
    {
        get => _hnswEnableExperimentalPersistence;
        set
        {
            _hnswEnableExperimentalPersistence = value;
            SetValue("hnsw_enable_experimental_persistence", value);
        }
    }
    string? _hnswEnableExperimentalPersistence;

    private void SetValue(string key, object? value)
    {
        this[key] = value;
    }
}