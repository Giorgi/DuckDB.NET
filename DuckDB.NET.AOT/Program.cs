using DuckDB.NET.Data;

namespace DuckDB.NET.AOT
{
    internal class Program
    {
        static void Main(string[] args)
        {
            using var conn = new DuckDBConnection(DuckDBConnectionStringBuilder.InMemoryConnectionString);
            conn.Open();
            using (var comm = conn.CreateCommand())
            {
                comm.CommandText = "SELECT 1";
                comm.ExecuteNonQuery();

                comm.CommandText = "SELECT 1";
                using (var reader = comm.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        _ = reader.GetInt32(0);
                    }
                }

                comm.CommandText = "SELECT [1,2,3]";
                using (var reader = comm.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.Write("[");
                        foreach (var item in (List<int>)reader[0])
                        {
                            Console.Write(item+",");
                        }
                        Console.WriteLine("]");
                    }
                }
                comm.CommandText = "select {\"a\":1,\"b\":2};";
                using (var reader = comm.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Console.Write("{");
                        foreach (var item in (Dictionary<string,object>)reader[0])
                        {
                            Console.Write($"{item.Key}={item.Value}, ");
                        }
                        Console.WriteLine("}");
                    }
                }
            }
        }
    }
}
