using System.Diagnostics;

namespace DuckDB.NET.Test;

// Memory tests measure the whole process, so they must not run alongside other tests.
[CollectionDefinition(nameof(MemoryTestsCollection), DisableParallelization = true)]
public class MemoryTestsCollection;

[Collection(nameof(MemoryTestsCollection))]
public class DuckDBDataReaderMemoryTests
{
    [Fact]
    public void NextResultReleasesPreviousResult()
    {
        // Each skipped result holds 2,000,000 BIGINT rows (about 16 MB), so a leak grows the process by
        // about 320 MB over 20 runs. Without a leak, growth stays well under the limit.
        const int runs = 20;
        const long maxGrowthMegabytes = 100;

        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        using var command = connection.CreateCommand();
        command.UseStreamingMode = false;
        command.CommandText = "SELECT * FROM range(2000000); SELECT 1";

        RunQuery();
        var before = ProcessMemoryMegabytes();

        for (var i = 0; i < runs; i++)
        {
            RunQuery();
        }

        var after = ProcessMemoryMegabytes();
        (after - before).Should().BeLessThan(maxGrowthMegabytes,
            "process memory went from {0} MB to {1} MB over {2} runs", before, after, runs);

        void RunQuery()
        {
            using var reader = command.ExecuteReader();
            reader.Read().Should().BeTrue();
            reader.NextResult().Should().BeTrue();
            reader.Read().Should().BeTrue();
            reader.GetInt32(0).Should().Be(1);
        }
    }

    // On Windows, private memory counts leaked pages even after the OS trims them from the working set.
    // Elsewhere private memory is not reliable, so use the working set.
    private static long ProcessMemoryMegabytes()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        using var process = Process.GetCurrentProcess();
        var bytes = OperatingSystem.IsWindows() ? process.PrivateMemorySize64 : process.WorkingSet64;
        return bytes / (1024 * 1024);
    }
}
