using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace DuckDB.NET.Data;

internal static class DuckDBSchema
{
    public static DataTable GetSchema(DuckDBConnection connection, string collectionName, string?[]? restrictionValues)
    {
        return collectionName.ToUpperInvariant() switch
        {
            "METADATACOLLECTIONS" => GetMetaDataCollections(),
            "RESTRICTIONS" => GetRestrictions(),
            "RESERVEDWORDS" => GetReservedWords(connection),
            "TABLES" => GetTables(connection, restrictionValues),
            _ => throw new ArgumentOutOfRangeException(nameof(collectionName), collectionName, "Invalid collection name.")
        };
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
                { DbMetaDataCollectionNames.MetaDataCollections, 0, 0 },
                { DbMetaDataCollectionNames.Restrictions, 0, 0 },
                { DbMetaDataCollectionNames.ReservedWords, 0, 0 },
                { "Tables", 4, 3 }
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
                { "Tables", "TableType", "table_type", 4 }
            }
        };

    private static DataTable GetReservedWords(DuckDBConnection connection)
    {
        var table = new DataTable(DbMetaDataCollectionNames.ReservedWords)
        {
            Columns = { { DbMetaDataColumnNames.ReservedWord, typeof(string) } }
        };

        using var command = connection.CreateCommand();
        command.CommandText = "SELECT keyword_name as ReservedWord FROM duckdb_keywords() WHERE keyword_category = 'reserved'";
        command.CommandType = CommandType.Text;
        LoadData(command, table);
        return table;
    }

    private static DataTable GetTables(DuckDBConnection connection, string?[]? restrictionValues)
    {
        var table = new DataTable("Tables")
        {
            Columns = { "table_catalog", "table_schema", "table_name", "table_type" }
        };

        const string query = "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables";

        using var command = BuildCommand(connection, query, restrictionValues, true,
            ["table_catalog", "table_schema", "table_name", "table_type"]);
        
        LoadData(command, table);
        return table;
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

    private static void LoadData(DuckDBCommand command, DataTable table)
    {
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
    }
}