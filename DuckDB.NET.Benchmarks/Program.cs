using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Toolchains.InProcess.Emit;
using DuckDB.NET.Benchmarks;

// The repo's Directory.Build.props renames the output assembly to DuckDB.NET.Benchmarks
// while the project file stays Benchmarks.csproj, so BenchmarkDotNet's default toolchain
// can't locate the csproj. Run in-process to avoid the separate build/spawn step.
var config = DefaultConfig.Instance
    .AddJob(Job.ShortRun.WithToolchain(InProcessEmitToolchain.Instance));

BenchmarkSwitcher
    .FromTypes([typeof(PreparedCommandBenchmark), typeof(PreparedCommandSetupBenchmark)])
    .Run(args, config);
