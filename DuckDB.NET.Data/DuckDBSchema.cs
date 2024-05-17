using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace DuckDB.NET.Data;

internal static class DuckDBSchema
{
    public static DataTable GetSchema(DuckDBConnection connection, string collectionName, string?[]? restrictionValues) =>
        collectionName.ToUpperInvariant() switch
        {
            "METADATACOLLECTIONS" => GetMetaDataCollections(),
            "RESTRICTIONS" => GetRestrictions(),
            "RESERVEDWORDS" => GetReservedWords(connection),
            "TABLES" => GetTables(connection, restrictionValues),
            "COLUMNS" => GetColumns(connection, restrictionValues),
            "FOREIGNKEYS" => GetForeignKeys(connection, restrictionValues),
            _ => throw new ArgumentOutOfRangeException(nameof(collectionName), collectionName, "Invalid collection name.")
        };

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
                { DbMetaDataCollectionNames.MetaDataCollections, 0, 0 },
                { DbMetaDataCollectionNames.Restrictions, 0, 0 },
                { DbMetaDataCollectionNames.ReservedWords, 0, 0 },
                { "Tables", 4, 3 },
                { "Columns", 4, 4 },
                { "ForeignKeys", 4, 3 }
            }
        };

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
                { "Tables", "Catalog", "table_catalog", 1 },
                { "Tables", "Schema", "table_schema", 2 },
                { "Tables", "Table", "table_name", 3 },
                { "Tables", "TableType", "table_type", 4 },

                { "Columns", "Catalog", "table_catalog", 1 },
                { "Columns", "Schema", "table_schema", 2 },
                { "Columns", "Table", "table_name", 3 },
                { "Columns", "Column", "column_name", 4 },

                { "ForeignKeys", "Catalog", "constraint_catalog", 1 },
                { "ForeignKeys", "Schema", "constraint_schema", 2 },
                { "ForeignKeys", "Table", "table_name", 3 },
                { "ForeignKeys", "Constraint", "constraint_name", 4 }
            },
        };

    private static DataTable GetReservedWords(DuckDBConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT keyword_name as ReservedWord FROM duckdb_keywords() WHERE keyword_category = 'reserved'";
        command.CommandType = CommandType.Text;
        return GetDataTable(DbMetaDataCollectionNames.ReservedWords, command);
    }

    private static DataTable GetTables(DuckDBConnection connection, string?[]? restrictionValues)
    {
        const string query = "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables";

        using var command = BuildCommand(connection, query, restrictionValues, true,
            ["table_catalog", "table_schema", "table_name", "table_type"]);

        return GetDataTable("Tables", command);
    }

    private static DataTable GetColumns(DuckDBConnection connection, string?[]? restrictionValues)
    {
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
        using var command = BuildCommand(connection, query, restrictionValues, true,
            ["table_catalog", "table_schema", "table_name", "column_name"]);
        
        return GetDataTable("Columns", command);
    } 
    
    private static DataTable GetForeignKeys(DuckDBConnection connection, string?[]? restrictionValues)
    {
        const string query =
            """
            SELECT 
                constraint_catalog, constraint_schema, constraint_name, 
                table_catalog, table_schema, table_name, constraint_type, 
                is_deferrable, initially_deferred 
            FROM information_schema.table_constraints
            WHERE constraint_type = 'FOREIGN KEY'
            """;
        using var command = BuildCommand(connection, query, restrictionValues, false,
            ["constraint_catalog", "constraint_schema", "table_name", "constraint_name"]);
        
        return GetDataTable("ForeignKeys", command);
    }

    private static DuckDBCommand BuildCommand(DuckDBConnection connection, string query, string?[]? restrictions,
        bool addWhere, string[]? restrictionNames)
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