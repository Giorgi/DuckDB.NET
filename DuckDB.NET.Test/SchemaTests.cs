using System;
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
            CREATE TABLE IF NOT EXISTS foo(foo_id INTEGER);
            CREATE TABLE IF NOT EXISTS bar(bar_id INTEGER);
            CREATE TABLE IF NOT EXISTS baz(baz_id INTEGER);
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
        Assert.Contains("select", schema.Rows.Cast<DataRow>().Select(c => c["ReservedWord"]));
    }

    [Fact]
    public void Tables()
    {
        var schema = Connection.GetSchema("Tables");
        Assert.Equal(3, schema.Rows.Count);
        var tableNames = schema.Rows.Cast<DataRow>().Select(x => (string)x["table_name"]);
        Assert.Equal(tableNames, ["bar", "baz", "foo"]);
    }
    
    [Fact]
    public void TablesWithRestrictions()
    {
        var schema = Connection.GetSchema("Tables", [null, null, "bar"]);
        Assert.Equal(1, schema.Rows.Count);
        Assert.Equal("bar", schema.Rows[0]["table_name"]);
    }
   
    [Fact]
    public void ColumnsWithRestrictions()
    {
        var schema = Connection.GetSchema("Columns", [null, null, "foo", "foo_id"]);
        Assert.Equal(1, schema.Rows.Count);
        Assert.Equal("foo", schema.Rows[0]["table_name"]);
        Assert.Equal("foo_id", schema.Rows[0]["column_name"]);
    }
}