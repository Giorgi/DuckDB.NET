using FluentAssertions;
using Xunit;

namespace DuckDB.NET.Test;

public class ScalarFunctionTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void RegisterScalarFunction()
    {
        var minValue = long.MaxValue;
        Connection.RegisterScalarFunction<long, long, long>("my_addition", (readers, writer, rowCount) =>
        {
            for (int index = 0; index < rowCount; index++)
            {
                var value = readers[0].GetValue<long>((ulong)index) + readers[1].GetValue<long>((ulong)index);
                writer.AppendValue(value, index);

                minValue = long.Min(minValue, value);
            }
        });

        Command.CommandText = "CREATE TABLE big_table AS SELECT (greatest(random(), 0.1) * 100)::BIGINT i FROM range(10000) t(i);";
        Command.ExecuteNonQuery();

        Command.CommandText = "SELECT MIN(my_addition(i, i)) FROM big_table;";
        var scalar = Command.ExecuteScalar();
        scalar.Should().Be(minValue);
    }
}