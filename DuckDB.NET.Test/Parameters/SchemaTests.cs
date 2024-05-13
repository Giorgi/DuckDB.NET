using System.Data;
using System.Data.Common;
using System.Linq;
using Xunit;

namespace DuckDB.NET.Test.Parameters;

public class SchemaTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
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
        Command.CommandText = "CREATE TABLE bar(key INTEGER)";
        Command.ExecuteNonQuery();

        var schema = Connection.GetSchema("Tables");
        Assert.Equal(1, schema.Rows.Count);
        Assert.Equal("bar", schema.Rows[0]["table_name"]);
    }
    
    [Fact]
    public void TablesWithRestrictions()
    {
        Command.CommandText = "CREATE TABLE foo(key INTEGER)";
        Command.ExecuteNonQuery();
        Command.CommandText = "CREATE TABLE bar(key INTEGER)";
        Command.ExecuteNonQuery();

        var schema = Connection.GetSchema("Tables", [null, null, "bar"]);
        Assert.Equal(1, schema.Rows.Count);
        Assert.Equal("bar", schema.Rows[0]["table_name"]);
    }
}