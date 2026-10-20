using System.Linq;
using System.IO;
using System.Text;
using DuckDB.NET.Native;

namespace DuckDB.NET.Data;

internal static class DuckDBSchema
{
    private static readonly string[] TableRestrictions = ["table_catalog", "table_schema", "table_name", "table_type"];

    private static readonly string[] ColumnRestrictions = ["table_catalog", "table_schema", "table_name", "column_name"];

    private static readonly string[] ForeignKeyRestrictions = ["constraint_catalog", "constraint_schema", "table_name", "constraint_name"];

    private static readonly string[] IndexesRestrictions = ["index_catalog", "index_schema", "table_name", "index_name"];

    public static DataTable GetSchema(DuckDBConnection connection, string collectionName, string?[]? restrictionValues)
    {
        ValidateRestrictions(collectionName, restrictionValues);
        return collectionName.ToUpperInvariant() switch
        {
            "METADATACOLLECTIONS" => GetMetaDataCollections(),
            "DATASOURCEINFORMATION" => GetDataSourceInformation(connection),
            "DATATYPES" => GetDataTypes(connection),
            "RESTRICTIONS" => GetRestrictions(),
            "RESERVEDWORDS" => GetReservedWords(connection),
            "TABLES" => GetTables(connection, restrictionValues),
            "COLUMNS" => GetColumns(connection, restrictionValues),
            "FOREIGNKEYS" => GetForeignKeys(connection, restrictionValues),
            "INDEXES" => GetIndexes(connection, restrictionValues),
            _ => throw new ArgumentOutOfRangeException(nameof(collectionName), collectionName, "Invalid collection name.")
        };
    }

    private static void ValidateRestrictions(string collectionName, string?[]? restrictionValues)
    {
        if (restrictionValues is { Length: > 0 }
            && collectionName.ToUpperInvariant() is "METADATACOLLECTIONS" or "DATASOURCEINFORMATION" or "DATATYPES" or "RESTRICTIONS" or "RESERVEDWORDS")
        {
            throw new ArgumentException("This collection does not support restrictions.", nameof(restrictionValues));
        }
    }

    private static DataTable GetMetaDataCollections() =>
        new(DbMetaDataCollectionNames.MetaDataCollections)
        {
            Columns =
            {
                { DbMetaDataColumnNames.CollectionName, typeof(string) },
                { DbMetaDataColumnNames.NumberOfRestrictions, typeof(int) },
                { DbMetaDataColumnNames.NumberOfIdentifierParts, typeof(int) }
            },
            Rows =
            {
                { DbMetaDataCollectionNames.DataSourceInformation, 0, 0 },
                { DbMetaDataCollectionNames.DataTypes, 0, 0 },
                { DbMetaDataCollectionNames.MetaDataCollections, 0, 0 },
                { DbMetaDataCollectionNames.ReservedWords, 0, 0 },
                { DbMetaDataCollectionNames.Restrictions, 0, 0 },
                { DuckDbMetaDataCollectionNames.Tables, TableRestrictions.Length, 3 },
                { DuckDbMetaDataCollectionNames.Columns, ColumnRestrictions.Length, 4 },
                { DuckDbMetaDataCollectionNames.ForeignKeys, ForeignKeyRestrictions.Length, 3 },
                { DuckDbMetaDataCollectionNames.Indexes, IndexesRestrictions.Length, 3 },
            }
        };

    private static DataTable GetDataSourceInformation(DuckDBConnection connection) =>
        new(DbMetaDataCollectionNames.DataSourceInformation)
        {
            Columns =
            {
                { DbMetaDataColumnNames.CompositeIdentifierSeparatorPattern, typeof(string) },
                { DbMetaDataColumnNames.DataSourceProductName, typeof(string) },
                { DbMetaDataColumnNames.DataSourceProductVersion, typeof(string) },
                { DbMetaDataColumnNames.DataSourceProductVersionNormalized, typeof(string) },
                { DbMetaDataColumnNames.GroupByBehavior, typeof(int) },
                { DbMetaDataColumnNames.IdentifierCase, typeof(int) },
                { DbMetaDataColumnNames.IdentifierPattern, typeof(string) },
                { DbMetaDataColumnNames.OrderByColumnsInSelect, typeof(bool) },
                { DbMetaDataColumnNames.ParameterMarkerFormat, typeof(string) },
                { DbMetaDataColumnNames.ParameterMarkerPattern, typeof(string) },
                { DbMetaDataColumnNames.ParameterNameMaxLength, typeof(int) },
                { DbMetaDataColumnNames.ParameterNamePattern, typeof(string) },
                { DbMetaDataColumnNames.QuotedIdentifierCase, typeof(int) },
                { DbMetaDataColumnNames.QuotedIdentifierPattern, typeof(string) },
                { DbMetaDataColumnNames.StatementSeparatorPattern, typeof(string) },
                { DbMetaDataColumnNames.StringLiteralPattern, typeof(string) },
                { DbMetaDataColumnNames.SupportedJoinOperators, typeof(int) }
            },
            Rows =
            {
                {
                    @"\.", "DuckDB", connection.ServerVersion, NormalizeVersion(connection.ServerVersion),
                    (int)GroupByBehavior.MustContainAll,
                    (int)IdentifierCase.Insensitive,
                    @"^[\p{L}_][\p{L}\p{N}_$]*$", false,
                    "${0}", @"(\$[\p{L}_][\p{L}\p{N}_]*)", DBNull.Value,
                    @"^[\p{L}_][\p{L}\p{N}_$]*$", (int)IdentifierCase.Insensitive,
                    "^(([^\\\"]|\\\"\\\")*)$", ";", @"'(([^']|'')*)'", (int)(SupportedJoinOperators.Inner | SupportedJoinOperators.LeftOuter
                        | SupportedJoinOperators.RightOuter | SupportedJoinOperators.FullOuter)
                }
            }
        };

    private static DataTable GetDataTypes(DuckDBConnection connection)
    {
        var table = new DataTable(DbMetaDataCollectionNames.DataTypes);
        foreach (var column in new (string Name, Type Type)[]
        {
            (DbMetaDataColumnNames.TypeName, typeof(string)), (DbMetaDataColumnNames.ProviderDbType, typeof(int)),
            (DbMetaDataColumnNames.DataType, typeof(string)), (DbMetaDataColumnNames.ColumnSize, typeof(long)),
            (DbMetaDataColumnNames.CreateFormat, typeof(string)), (DbMetaDataColumnNames.CreateParameters, typeof(string)),
            (DbMetaDataColumnNames.IsAutoIncrementable, typeof(bool)), (DbMetaDataColumnNames.IsBestMatch, typeof(bool)),
            (DbMetaDataColumnNames.IsCaseSensitive, typeof(bool)), (DbMetaDataColumnNames.IsConcurrencyType, typeof(bool)),
            (DbMetaDataColumnNames.IsFixedLength, typeof(bool)), (DbMetaDataColumnNames.IsFixedPrecisionScale, typeof(bool)),
            (DbMetaDataColumnNames.IsLiteralSupported, typeof(bool)), (DbMetaDataColumnNames.IsLong, typeof(bool)),
            (DbMetaDataColumnNames.IsNullable, typeof(bool)), (DbMetaDataColumnNames.IsSearchable, typeof(bool)),
            (DbMetaDataColumnNames.IsSearchableWithLike, typeof(bool)), (DbMetaDataColumnNames.IsUnsigned, typeof(bool)),
            (DbMetaDataColumnNames.LiteralPrefix, typeof(string)), (DbMetaDataColumnNames.LiteralSuffix, typeof(string)),
            (DbMetaDataColumnNames.MaximumScale, typeof(short)), (DbMetaDataColumnNames.MinimumScale, typeof(short)),
            ("NativeDataType", typeof(string))
        })
        {
            table.Columns.Add(column.Name, column.Type);
        }

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT DISTINCT type_name, logical_type FROM duckdb_types() ORDER BY type_name";
        using var reader = command.ExecuteReader();
        var types = new Dictionary<string, (string TypeName, string LogicalType)>(StringComparer.OrdinalIgnoreCase);
        while (reader.Read())
        {
            var typeName = reader.GetString(0);
            var logicalTypeName = reader.GetString(1);

            if (!types.TryGetValue(typeName, out var existing)
                || typeName.Equals(logicalTypeName, StringComparison.Ordinal) && !existing.TypeName.Equals(existing.LogicalType, StringComparison.Ordinal))
            {
                types[typeName] = (typeName, logicalTypeName);
            }
        }

        foreach (var (typeName, logicalTypeName) in types.Values.OrderBy(type => type.TypeName, StringComparer.OrdinalIgnoreCase))
        {
            var providerType = ParseDuckDBType(logicalTypeName);

            AddType(
                table, typeName, providerType, GetClrType(providerType),
                createFormat: providerType == DuckDBType.Decimal ? "DECIMAL({0},{1})" : typeName,
                createParameters: providerType == DuckDBType.Decimal ? "precision,scale" : null,
                isUnsigned: providerType is DuckDBType.UnsignedTinyInt or DuckDBType.UnsignedSmallInt
                    or DuckDBType.UnsignedInteger or DuckDBType.UnsignedBigInt or DuckDBType.UnsignedHugeInt,
                isCaseSensitive: providerType == DuckDBType.Varchar,
                isLong: providerType == DuckDBType.Blob,
                isBestMatch: typeName.Equals(logicalTypeName, StringComparison.OrdinalIgnoreCase),
                nativeTypeName: logicalTypeName);
        }

        return table;
    }

    private static DuckDBType ParseDuckDBType(string typeName) => typeName.ToUpperInvariant().Replace(" WITH TIME ZONE", "_TZ") switch
    {
        "BOOLEAN" => DuckDBType.Boolean,
        "TINYINT" => DuckDBType.TinyInt,
        "SMALLINT" => DuckDBType.SmallInt,
        "INTEGER" => DuckDBType.Integer,
        "BIGINT" => DuckDBType.BigInt,
        "UTINYINT" => DuckDBType.UnsignedTinyInt,
        "USMALLINT" => DuckDBType.UnsignedSmallInt,
        "UINTEGER" => DuckDBType.UnsignedInteger,
        "UBIGINT" => DuckDBType.UnsignedBigInt,
        "FLOAT" => DuckDBType.Float,
        "DOUBLE" => DuckDBType.Double,
        "DECIMAL" => DuckDBType.Decimal,
        "VARCHAR" => DuckDBType.Varchar,
        "BLOB" => DuckDBType.Blob,
        "UUID" => DuckDBType.Uuid,
        "DATE" => DuckDBType.Date,
        "TIME" => DuckDBType.Time,
        "TIME_TZ" => DuckDBType.TimeTz,
        "TIMESTAMP" => DuckDBType.Timestamp,
        "TIMESTAMP_S" => DuckDBType.TimestampS,
        "TIMESTAMP_MS" => DuckDBType.TimestampMs,
        "TIMESTAMP_NS" => DuckDBType.TimestampNs,
        "TIMESTAMP_TZ" => DuckDBType.TimestampTz,
        "INTERVAL" => DuckDBType.Interval,
        "HUGEINT" => DuckDBType.HugeInt,
        "UHUGEINT" => DuckDBType.UnsignedHugeInt,
        "VARINT" or "BIGNUM" => DuckDBType.VarInt,
        "LIST" => DuckDBType.List,
        "ARRAY" => DuckDBType.Array,
        "STRUCT" => DuckDBType.Struct,
        "MAP" => DuckDBType.Map,
        "UNION" => DuckDBType.Union,
        "ENUM" => DuckDBType.Enum,
        "BIT" => DuckDBType.Bit,
        "ANY" => DuckDBType.Any,
        "SQLNULL" or "NULL" => DuckDBType.SqlNull,
        "STRING_LITERAL" => DuckDBType.StringLiteral,
        "INTEGER_LITERAL" => DuckDBType.IntegerLiteral,
        "TIME_NS" => DuckDBType.TimeNs,
        "GEOMETRY" => DuckDBType.Geometry,
        "VARIANT" => DuckDBType.Variant,
        _ => DuckDBType.Invalid
    };

    private static Type GetClrType(DuckDBType type) => type switch
    {
        DuckDBType.Boolean => typeof(bool),
        DuckDBType.TinyInt => typeof(sbyte),
        DuckDBType.SmallInt => typeof(short),
        DuckDBType.Integer => typeof(int),
        DuckDBType.BigInt => typeof(long),
        DuckDBType.UnsignedTinyInt => typeof(byte),
        DuckDBType.UnsignedSmallInt => typeof(ushort),
        DuckDBType.UnsignedInteger => typeof(uint),
        DuckDBType.UnsignedBigInt => typeof(ulong),
        DuckDBType.Float => typeof(float),
        DuckDBType.Double => typeof(double),
        DuckDBType.Decimal => typeof(decimal),
        DuckDBType.Varchar => typeof(string),
        DuckDBType.Blob => typeof(Stream),
        DuckDBType.Uuid => typeof(Guid),
        DuckDBType.Date => typeof(DateOnly),
        DuckDBType.Time => typeof(TimeOnly),
        DuckDBType.TimeTz => typeof(DateTimeOffset),
        DuckDBType.TimestampTz => typeof(DateTime),
        DuckDBType.Timestamp or DuckDBType.TimestampS or DuckDBType.TimestampMs or DuckDBType.TimestampNs => typeof(DateTime),
        DuckDBType.Interval => typeof(TimeSpan),
        DuckDBType.HugeInt or DuckDBType.UnsignedHugeInt or DuckDBType.VarInt => typeof(System.Numerics.BigInteger),
        DuckDBType.Enum or DuckDBType.Bit => typeof(string),
        DuckDBType.SqlNull => typeof(DBNull),
        _ => typeof(object)
    };

    private static void AddType(
        DataTable table,
        string typeName,
        DuckDBType providerType,
        Type dataType,
        long columnSize = -1,
        string? createFormat = null,
        string? createParameters = null,
        bool isUnsigned = false,
        bool isCaseSensitive = false,
        bool isLong = false,
        bool isBestMatch = false,
        string? nativeTypeName = null)
    {
        table.Rows.Add(
            typeName, (int)providerType, dataType.FullName!, columnSize,
            createFormat ?? (object)DBNull.Value, createParameters ?? (object)DBNull.Value,
            false, isBestMatch, isCaseSensitive, false, false, false, true, isLong,
            true, true, isCaseSensitive, isUnsigned ? (object)true : DBNull.Value,
            DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, nativeTypeName ?? typeName);
    }

    private static string NormalizeVersion(string serverVersion)
    {
        var value = serverVersion.TrimStart('v', 'V');
        return Version.TryParse(value, out var version)
            ? $"{version.Major:00}.{version.Minor:000}.{(version.Build < 0 ? 0 : version.Build):0000}.{(version.Revision < 0 ? 0 : version.Revision):0000}"
            : serverVersion;
    }

    private static DataTable GetRestrictions() =>
        new(DbMetaDataCollectionNames.Restrictions)
        {
            Columns =
            {
                { "CollectionName", typeof(string) },
                { "RestrictionName", typeof(string) },
                { "RestrictionDefault", typeof(string) },
                { "RestrictionNumber", typeof(int) }
            },
            Rows =
            {
                { DuckDbMetaDataCollectionNames.Tables, "Catalog", "table_catalog", 1 },
                { DuckDbMetaDataCollectionNames.Tables, "Schema", "table_schema", 2 },
                { DuckDbMetaDataCollectionNames.Tables, "Table", "table_name", 3 },
                { DuckDbMetaDataCollectionNames.Tables, "TableType", "table_type", 4 },

                { DuckDbMetaDataCollectionNames.Columns, "Catalog", "table_catalog", 1 },
                { DuckDbMetaDataCollectionNames.Columns, "Schema", "table_schema", 2 },
                { DuckDbMetaDataCollectionNames.Columns, "Table", "table_name", 3 },
                { DuckDbMetaDataCollectionNames.Columns, "Column", "column_name", 4 },

                { DuckDbMetaDataCollectionNames.ForeignKeys, "Catalog", "constraint_catalog", 1 },
                { DuckDbMetaDataCollectionNames.ForeignKeys, "Schema", "constraint_schema", 2 },
                { DuckDbMetaDataCollectionNames.ForeignKeys, "Table", "table_name", 3 },
                { DuckDbMetaDataCollectionNames.ForeignKeys, "Constraint", "constraint_name", 4 },

                { DuckDbMetaDataCollectionNames.Indexes, "Catalog", "index_catalog", 1 },
                { DuckDbMetaDataCollectionNames.Indexes, "Schema", "index_schema", 2 },
                { DuckDbMetaDataCollectionNames.Indexes, "Table", "table_name", 3 },
                { DuckDbMetaDataCollectionNames.Indexes, "Index", "index_name", 4 },
            },
        };

    private static DataTable GetReservedWords(DuckDBConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT keyword_name as ReservedWord FROM duckdb_keywords() WHERE keyword_category = 'reserved'";
        return GetDataTable(DbMetaDataCollectionNames.ReservedWords, command);
    }

    private static DataTable GetTables(DuckDBConnection connection, string?[]? restrictionValues)
    {
        if (restrictionValues?.Length > TableRestrictions.Length)
        {
            throw new ArgumentException("Too many restrictions", nameof(restrictionValues));
        }

        const string query = "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables";

        using var command = BuildCommand(connection, query, restrictionValues, true, TableRestrictions);

        return GetDataTable(DuckDbMetaDataCollectionNames.Tables, command);
    }

    private static DataTable GetColumns(DuckDBConnection connection, string?[]? restrictionValues)
    {
        if (restrictionValues?.Length > ColumnRestrictions.Length)
        {
            throw new ArgumentException("Too many restrictions", nameof(restrictionValues));
        }

        const string query =
            """
            SELECT
                table_catalog, table_schema, table_name, column_name,
                ordinal_position, column_default, is_nullable, data_type,
                character_maximum_length, character_octet_length,
                numeric_precision, numeric_precision_radix,
                numeric_scale, datetime_precision,
                character_set_catalog, character_set_schema, character_set_name, collation_catalog 
            FROM information_schema.columns 
            """;
        using var command = BuildCommand(connection, query, restrictionValues, true, ColumnRestrictions);
        
        return GetDataTable(DuckDbMetaDataCollectionNames.Columns, command);
    } 
    
    private static DataTable GetForeignKeys(DuckDBConnection connection, string?[]? restrictionValues)
    {
        if (restrictionValues?.Length > ForeignKeyRestrictions.Length)
        {
            throw new ArgumentException("Too many restrictions", nameof(restrictionValues));
        }

        const string query =
            """
            SELECT 
                constraint_catalog, constraint_schema, constraint_name, 
                table_catalog, table_schema, table_name, constraint_type, 
                is_deferrable, initially_deferred 
            FROM information_schema.table_constraints
            WHERE constraint_type = 'FOREIGN KEY'
            """;
        using var command = BuildCommand(connection, query, restrictionValues, false, ForeignKeyRestrictions);
        
        return GetDataTable(DuckDbMetaDataCollectionNames.ForeignKeys, command);
    }
    
    private static DataTable GetIndexes(DuckDBConnection connection, string?[]? restrictionValues)
    {
        if (restrictionValues?.Length > IndexesRestrictions.Length)
        {
            throw new ArgumentException("Too many restrictions", nameof(restrictionValues));
        }

        const string query =
            """
            SELECT
            	database_name as index_catalog,
            	schema_name as index_schema,
            	index_name,
            	table_name,
            	is_unique,
            	is_primary
            FROM duckdb_indexes()
            """;
        using var command = BuildCommand(connection, query, restrictionValues, true, IndexesRestrictions);
        
        return GetDataTable(DuckDbMetaDataCollectionNames.Indexes, command);
    }

    private static DuckDBCommand BuildCommand(DuckDBConnection connection, string query, string?[]? restrictions, bool addWhere, string[]? restrictionNames)
    {
        var command = connection.CreateCommand();
        if (restrictions is not { Length: > 0 } || restrictionNames == null)
        {
            command.CommandText = query;
            return command;
        }

        var builder = new StringBuilder(query);
        foreach (var (name, restriction) in restrictionNames.Zip(restrictions, Tuple.Create))
        {
            if (restriction?.Length > 0)
            {
                if (addWhere)
                {
                    builder.Append(" WHERE ");
                    addWhere = false;
                }
                else
                {
                    builder.Append(" AND ");
                }

                builder.Append($"{name} = ${name}");
                command.Parameters.Add(new DuckDBParameter(name, restriction));
            }
        }

        command.CommandText = builder.ToString();
        return command;
    }

    private static DataTable GetDataTable(string tableName, DuckDBCommand command)
    {
        var table = new DataTable(tableName);
        try
        {
            using var reader = command.ExecuteReader();
            table.BeginLoadData();
            table.Load(reader);
        }
        finally
        {
            table.EndLoadData();
        }

        return table;
    }
}
