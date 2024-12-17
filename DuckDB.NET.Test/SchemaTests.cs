using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Xunit;

namespace DuckDB.NET.Test;

public class SchemaTests : DuckDBTestBase
{
    public SchemaTests(DuckDBDatabaseFixture db) : base(db)
    {
        Command.CommandText =
            """
            CREATE TABLE IF NOT EXISTS foo(foo_id INTEGER PRIMARY KEY, name VARCHAR(100), date DATE UNIQUE);
            CREATE TABLE IF NOT EXISTS bar(bar_id INTEGER PRIMARY KEY, name VARCHAR(100), date DATE UNIQUE, foo_id INTEGER REFERENCES foo(foo_id));
            CREATE TABLE IF NOT EXISTS baz(baz_id INTEGER PRIMARY KEY, bar_id INTEGER REFERENCES bar(bar_id));
            """;
        Command.ExecuteNonQuery();
    }

    [Fact]
    public void InvalidCollection()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => Connection.GetSchema("Invalid"));
    }
    
    [Fact]
    public void MetaDataCollections()
    {
        var schema = Connection.GetSchema(DbMetaDataCollectionNames.MetaDataCollections);
        Assert.NotEmpty(schema.Rows);

        foreach (DataRow row in schema.Rows)
        {
            var collectionName = (string)row!["CollectionName"];
            Assert.NotNull(Connection.GetSchema(collectionName));
        }
    }
    
    [Fact]
    public void NoParametersShouldBeEquivalentToMetaDataCollections()
    {
        var actualSchema = Connection.GetSchema();
        var actual = actualSchema.Rows
            .Cast<DataRow>()
            .Select(r => (string)r["CollectionName"])
            .ToList();

        var expectedSchema = Connection.GetSchema(DbMetaDataCollectionNames.MetaDataCollections);
        var expected = expectedSchema.Rows
            .Cast<DataRow>()
            .Select(r => (string)r["CollectionName"])
            .ToList();

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Restrictions()
    {
        var restrictions = Connection.GetSchema(DbMetaDataCollectionNames.Restrictions);
        Assert.NotEmpty(restrictions.Rows);
        
        var collections = Connection.GetSchema(DbMetaDataCollectionNames.MetaDataCollections);
        var restrictionsByCollectionName = restrictions.Rows.Cast<DataRow>()
            .GroupBy(r => r["CollectionName"])
            .ToDictionary(g => g.Key, g => g.Count());
        
        foreach (DataRow row in collections.Rows)
        {
            var collectionName = (string)row["CollectionName"];
            var numberOfRestrictions = (int)row["NumberOfRestrictions"];

            if (numberOfRestrictions != 0)
            {
                Assert.Equal(numberOfRestrictions, restrictionsByCollectionName[collectionName]);
            }
            else
            {
                Assert.DoesNotContain(collectionName, restrictionsByCollectionName.Keys);
            }
        }
    }

    [Fact]
    public void ReservedWords()
    {
        var schema = Connection.GetSchema(DbMetaDataCollectionNames.ReservedWords);
        Assert.NotEmpty(schema.Rows);
        Assert.Contains("select", GetValues(schema, "ReservedWord"));
    }

    [Fact]
    public void Tables()
    {
        var schema = Connection.GetSchema("Tables");
        Assert.Equal(3, schema.Rows.Count);
        var tableNames = GetValues(schema, "table_name");
        Assert.Equal(["bar", "baz", "foo"], tableNames);
    }

    [Fact]
    public void TablesWithRestrictions()
    {
        var schema = Connection.GetSchema("Tables", [null, null, "bar"]);
        Assert.Equal(1, schema.Rows.Count);
        Assert.Equal("bar", schema.Rows[0]["table_name"]);
    }
 
    [Fact]
    public void TablesTooManyRestrictions()
    {
        Assert.Throws<ArgumentException>(() => Connection.GetSchema("Tables", new string [5]));
    }
  
    [Fact]
    public void ColumnsWithRestrictions()
    {
        var schema = Connection.GetSchema("Columns", [null, null, "foo", "foo_id"]);
        Assert.Equal(1, schema.Rows.Count);
        Assert.Equal("foo", schema.Rows[0]["table_name"]);
        Assert.Equal("foo_id", schema.Rows[0]["column_name"]);
    }

    [Fact]
    public void ColumnsTooManyRestrictions()
    {
        Assert.Throws<ArgumentException>(() => Connection.GetSchema("Columns", new string [5]));
    }

    [Fact]
    public void ForeignKeysWithRestrictions()
    {
        var schema = Connection.GetSchema("ForeignKeys", [null, null, "bar", null]);
        Assert.Equal(1, schema.Rows.Count);
        Assert.Equal("bar", schema.Rows[0]["table_name"]);
    }

    [Fact]
    public void ForeignKeysTooManyRestrictions()
    {
        Assert.Throws<ArgumentException>(() => Connection.GetSchema("ForeignKeys", new string [5]));
    }

    [Fact]
    public void NonUniqueIndex()
    {
        Command.CommandText = "CREATE INDEX bar_name_ix ON bar(name);";
        Command.ExecuteNonQuery();

        var schema = Connection.GetSchema("Indexes", [null, null, "bar", null]);
        Assert.Equal(1, schema.Rows.Count);
        Assert.Equal("bar_name_ix", schema.Rows[0]["index_name"]);
        Assert.Equal(false, schema.Rows[0]["is_unique"]);
        Assert.Equal(false, schema.Rows[0]["is_primary"]);
    }

    [Fact]
    public void UniqueIndex()
    {
        Command.CommandText = "CREATE UNIQUE INDEX foo_name_uq ON foo(name);";
        Command.ExecuteNonQuery();

        var schema = Connection.GetSchema("Indexes", [null, null, "foo", null]);
        Assert.Equal(1, schema.Rows.Count);
        Assert.Equal("foo_name_uq", schema.Rows[0]["index_name"]);
        Assert.Equal(true, schema.Rows[0]["is_unique"]);
        Assert.Equal(false, schema.Rows[0]["is_primary"]);
    }

    [Fact]
    public void IndexesTooManyRestrictions()
    {
        Assert.Throws<ArgumentException>(() => Connection.GetSchema("Indexes", new string [5]));
    }

    [Fact]
    public void DataSourceInformation() {
        var schema = Connection.GetSchema(DbMetaDataCollectionNames.DataSourceInformation);
        Assert.NotEmpty(schema.Rows);

        Assert.Equal(1, schema.Rows.Count);
        Assert.Equal("\\.", schema.Rows[0][DbMetaDataColumnNames.CompositeIdentifierSeparatorPattern]);
        Assert.Equal("duckdb", schema.Rows[0][DbMetaDataColumnNames.DataSourceProductName]);
        Assert.Equal(Connection.ServerVersion, schema.Rows[0][DbMetaDataColumnNames.DataSourceProductVersion]);
        Assert.Equal(Connection.ServerVersion, schema.Rows[0][DbMetaDataColumnNames.DataSourceProductVersionNormalized]);
        Assert.Equal(GroupByBehavior.Unrelated, (GroupByBehavior)schema.Rows[0][DbMetaDataColumnNames.GroupByBehavior]);
        Assert.Equal("(^\\[\\p{Lo}\\p{Lu}\\p{Ll}_@#][\\p{Lo}\\p{Lu}\\p{Ll}\\p{Nd}@$#_]*$)|(^\\[[^\\]\\0]|\\]\\]+\\]$)|(^\\\"[^\\\"\\0]|\\\"\\\"+\\\"$)", schema.Rows[0][DbMetaDataColumnNames.IdentifierPattern]);
        Assert.Equal(IdentifierCase.Insensitive, (IdentifierCase)schema.Rows[0][DbMetaDataColumnNames.IdentifierCase]);
        Assert.Equal(false, schema.Rows[0][DbMetaDataColumnNames.OrderByColumnsInSelect]);
        Assert.Equal("{0}", schema.Rows[0][DbMetaDataColumnNames.ParameterMarkerFormat]);
        Assert.Equal("$[\\p{Lo}\\p{Lu}\\p{Ll}\\p{Lm}_@#][\\p{Lo}\\p{Lu}\\p{Ll}\\p{Lm}\\p{Nd}\\uff3f_@#\\$]*(?=\\s+|$)", schema.Rows[0][DbMetaDataColumnNames.ParameterMarkerPattern]);
        Assert.Equal(128, schema.Rows[0][DbMetaDataColumnNames.ParameterNameMaxLength]);
        Assert.Equal("^[\\p{Lo}\\p{Lu}\\p{Ll}\\p{Lm}_@#][\\p{Lo}\\p{Lu}\\p{Ll}\\p{Lm}\\p{Nd}\\uff3f_@#\\$]*(?=\\s+|$)", schema.Rows[0][DbMetaDataColumnNames.ParameterNamePattern]);
        Assert.Equal("(([^\\[]|\\]\\])*)", schema.Rows[0][DbMetaDataColumnNames.QuotedIdentifierPattern]);
        Assert.Equal(IdentifierCase.Insensitive, (IdentifierCase)schema.Rows[0][DbMetaDataColumnNames.QuotedIdentifierCase]);
        Assert.Equal(";", schema.Rows[0][DbMetaDataColumnNames.StatementSeparatorPattern]);
        Assert.Equal("'(([^']|'')*)'", schema.Rows[0][DbMetaDataColumnNames.StringLiteralPattern]);
        Assert.Equal(SupportedJoinOperators.Inner | SupportedJoinOperators.LeftOuter | SupportedJoinOperators.RightOuter | SupportedJoinOperators.FullOuter, (SupportedJoinOperators)schema.Rows[0][DbMetaDataColumnNames.SupportedJoinOperators]);
    }

    private static IEnumerable<string> GetValues(DataTable schema, string columnName) =>
        schema.Rows.Cast<DataRow>().Select(r => (string)r[columnName]);
}