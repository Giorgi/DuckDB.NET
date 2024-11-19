using DuckDB.NET.Data;

namespace AotTestApp;

internal class Program
{
    static void Main(string[] args)
    {
        using (var cn = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString))
        {
            cn.Open();
            using var duckDBCommand = cn.CreateCommand();
            duckDBCommand.CommandText = "Select struct from test_all_types()";

            using var reader = duckDBCommand.ExecuteReader();
            while (reader.Read())
            {
                if (!reader.IsDBNull(0))
                {
                    var structTest = reader.GetFieldValue<StructTest>(0);
                    Console.WriteLine($"A: {structTest.A}, B: {structTest.B?.Length}");
                }
            }
        }

        Console.ReadKey();
    }
}

class StructTest
{
    public int? A { get; set; }
    public string B { get; set; }
}