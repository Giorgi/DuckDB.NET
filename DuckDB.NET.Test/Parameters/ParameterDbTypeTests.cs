namespace DuckDB.NET.Test.Parameters;

public class ParameterDbTypeTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void DbTypeSetBeforeTheValueIsKept()
    {
        var parameter = new DuckDBParameter { DbType = DbType.Time };

        parameter.Value = new TimeSpan(13, 45, 30);

        parameter.DbType.Should().Be(DbType.Time);
    }

    [Fact]
    public void DbTypeSetAfterTheValueIsKept()
    {
        var parameter = new DuckDBParameter { Value = new TimeSpan(13, 45, 30) };

        parameter.DbType = DbType.Time;
        parameter.Value = new TimeSpan(1, 2, 3);

        parameter.DbType.Should().Be(DbType.Time);
    }

    [Fact]
    public void DbTypeIsInferredFromTheValueWhenNoneWasSet()
    {
        var parameter = new DuckDBParameter();

        parameter.Value = 5;
        parameter.DbType.Should().Be(DbType.Int32);

        parameter.Value = "text";
        parameter.DbType.Should().Be(DbType.String);

        parameter.Value = 5L;
        parameter.DbType.Should().Be(DbType.Int64);
    }

    [Fact]
    public void AssigningTheSameValueDoesNotChangeItsDbType()
    {
        var parameter = new DuckDBParameter(5);
        var value = parameter.Value;

        parameter.Value = value;

        parameter.DbType.Should().Be(DbType.Int32);
    }

    [Fact]
    public void ResetDbTypeReturnsToInference()
    {
        var parameter = new DuckDBParameter { DbType = DbType.Time };

        parameter.ResetDbType();
        parameter.Value = 5;

        parameter.DbType.Should().Be(DbType.Int32);
    }

    [Fact]
    public void ConstructorsThatTakeAValueStillInferItsType()
    {
        new DuckDBParameter(5).DbType.Should().Be(DbType.Int32);
        new DuckDBParameter("name", "text").DbType.Should().Be(DbType.String);

        var typed = new DuckDBParameter(DbType.Int64, 5);
        typed.Value = "text";
        typed.DbType.Should().Be(DbType.Int64);
    }

    [Fact]
    public void ATimeSpanWithTheTimeDbTypeIsBoundAsATimeInAMergeSource()
    {
        Command.CommandText = "CREATE OR REPLACE TABLE parameter_dbtype_time (id INTEGER PRIMARY KEY, v TIME)";
        Command.ExecuteNonQuery();

        // A MERGE source's SELECT gives DuckDB no target column to infer the parameter's type from.
        Command.CommandText = "MERGE INTO parameter_dbtype_time t USING (SELECT $1 AS id, $2 AS v) s ON t.id = s.id " +
                              "WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)";
        Command.Parameters.Add(new DuckDBParameter { Value = 1 });
        Command.Parameters.Add(new DuckDBParameter { DbType = DbType.Time, Value = new TimeSpan(13, 45, 30) });
        Command.ExecuteNonQuery();

        Command.Parameters.Clear();
        Command.CommandText = "SELECT v FROM parameter_dbtype_time WHERE id = 1";
        Command.ExecuteScalar().Should().Be(new TimeOnly(13, 45, 30));
    }

    [Fact]
    public void ATimeSpanWithoutADbTypeIsStillBoundAsAnInterval()
    {
        Command.CommandText = "SELECT ?";
        Command.Parameters.Add(new DuckDBParameter { Value = new TimeSpan(3, 4, 5, 6) });

        Command.ExecuteScalar().Should().Be(new TimeSpan(3, 4, 5, 6));
    }
}
