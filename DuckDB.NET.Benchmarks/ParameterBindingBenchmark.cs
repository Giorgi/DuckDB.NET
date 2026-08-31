using System.Text;
using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;

namespace DuckDB.NET.Benchmarks;

[MemoryDiagnoser]
public class ParameterBindingBenchmark
{
    private DuckDBConnection connection = null!;
    private DuckDBCommand exact = null!;
    private DuckDBCommand prefixed = null!;
    private DuckDBCommand positional = null!;

    [Params(1, 8, 32)]
    public int ParameterCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        exact = BuildNamed("");
        prefixed = BuildNamed("$");
        positional = BuildPositional();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        exact.Dispose();
        prefixed.Dispose();
        positional.Dispose();
        connection.Dispose();
    }

    [Benchmark(Baseline = true)]
    public object? NamedExact() => exact.ExecuteScalar();

    [Benchmark]
    public object? NamedPrefixed() => prefixed.ExecuteScalar();

    [Benchmark]
    public object? Positional() => positional.ExecuteScalar();

    private DuckDBCommand BuildNamed(string parameterNamePrefix)
    {
        var command = connection.CreateCommand();
        var sql = new StringBuilder("SELECT ");

        for (var i = 0; i < ParameterCount; i++)
        {
            if (i > 0)
            {
                sql.Append(" + ");
            }

            sql.Append("$p").Append(i).Append("::INT");
            command.Parameters.Add(new DuckDBParameter($"{parameterNamePrefix}p{i}", i));
        }

        command.CommandText = sql.ToString();
        return command;
    }

    private DuckDBCommand BuildPositional()
    {
        var command = connection.CreateCommand();
        var sql = new StringBuilder("SELECT ");

        for (var i = 0; i < ParameterCount; i++)
        {
            if (i > 0)
            {
                sql.Append(" + ");
            }

            sql.Append("?::INT");
            command.Parameters.Add(new DuckDBParameter(i));
        }

        command.CommandText = sql.ToString();
        return command;
    }
}
