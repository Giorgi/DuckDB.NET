namespace DuckDB.NET.Test;

public class ExtractStatementsTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void MultipleInsertsIntoTable()
    {
        Command.CommandText = "CREATE TABLE Test(foo INTEGER, bar INTEGER);";
        Command.ExecuteNonQuery();

        Command.CommandText = "Insert into Test (foo, bar) values (1,2); Insert into Test (foo, bar) values (3,4);";
        Command.ExecuteNonQuery().Should().Be(2);

        Command.CommandText = "Select * from Test";
        var dataReader = Command.ExecuteReader();

        dataReader.Read();
        dataReader.GetInt32(0).Should().Be(1);
        dataReader.GetInt32(1).Should().Be(2);
        
        dataReader.Read();
        dataReader.GetInt32(0).Should().Be(3);
        dataReader.GetInt32(1).Should().Be(4);
    }

    [Fact]
    public void WrongCommandThrowsException()
    {
        Command.CommandText = "error";
        
        Command.Invoking(cmd => cmd.ExecuteNonQuery()).Should()
               .Throw<DuckDBException>().Where(e => e.Message.Contains("syntax error at or near"));
    }

    [Fact]
    public void NotExistingTableThrowsException()
    {
        Command.CommandText = "Select 2; Select 1 from dummy";
        
        Command.Invoking(cmd => cmd.ExecuteNonQuery()).Should()
               .Throw<DuckDBException>().Where(e => e.Message.Contains("Table with name dummy does not exist"));
    }

    [Fact]
    public void MissingParametersThrowsException()
    {
        Command.CommandText = "Select ?1::integer; Select ?1::integer, ?2::integer";
        Command.Parameters.Add(new DuckDBParameter(42));

        var dataReader = Command.ExecuteReader();

        dataReader.Invoking(reader => reader.NextResult()).Should()
            .Throw<InvalidOperationException>().Where(e => e.Message.Contains("Invalid number of parameters. Expected 2, got 1"));
    }
}