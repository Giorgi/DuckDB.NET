using System;
using System.Data;
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
            "TABLES" => GetTables(connection, restrictionValues),
            _ => throw new ArgumentOutOfRangeException(nameof(collectionName), collectionName, "Invalid collection name.")
        };
    }

    private static DataTable GetMetaDataCollections() =>
        new("MetaDataCollections")
        {
            Columns =
            {
                { "CollectionName", typeof(string) },
                { "NumberOfRestrictions", typeof(int) },
                { "NumberOfIdentifierParts", typeof(int) }
            },
            Rows =
            {
                { "MetaDataCollections", 0, 0 },
                { "Restrictions", 0, 0 },
                { "Tables", 4, 3 }
            }
        };

    private static DataTable GetRestrictions() =>
        new("Restrictions")
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

    private static DataTable GetTables(DuckDBConnection connection, string?[]? restrictionValues)
    {
        var table = new DataTable("Tables")
        {
            Columns =
            {
                "table_catalog",
                "table_schema",
                "table_name",
                "table_type"
            }
        };

        const string query = "SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables";

        var command = BuildCommand(connection, query, restrictionValues, true,
            "table_catalog", "table_schema", "table_name", "table_type");
        var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var values = new object[reader.FieldCount];
            reader.GetValues(values);
            table.Rows.Add(values);
        }

        return table;
    }

    private static DuckDBCommand BuildCommand(DuckDBConnection connection, string query, string?[]? restrictions, bool addWhere, params string[]? names)
    {
        var command = connection.CreateCommand();
        if (restrictions == null || names == null)
        {
            command.CommandText = query;
            return command;
        }

        var builder = new StringBuilder(query);
        foreach (var (name, restriction) in names.Zip(restrictions, Tuple.Create))
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
}