using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using DuckDB.NET.Benchmarks;

// Every benchmark runs twice, each in its own process: against a published package (the baseline) and against
// the code in this repository. The project file switches between the two on UseNuGet.
//
// The baseline is a released version from nuget.org, such as "1.5.6", or a build of the develop branch from
// GitHub Packages, such as "1.5.6-alpha.35". The second kind needs the "github" source in nuget.config.
const string baselineVersion = "1.5.6";

// --aa runs the baseline against itself instead of against this repository: the ratios then show how much two
// identical builds differ on this machine, which is the noise every other run has to be read against.
var aa = args.Contains("--aa");
args = args.Where(arg => arg != "--aa").ToArray();

var baseline = Job.Default.WithMsBuildArguments("/p:UseNuGet=true", $"/p:DuckDBNuGetVersion={baselineVersion}");

var config = DefaultConfig.Instance
    .AddJob(baseline.WithId(baselineVersion).AsBaseline())
    .AddJob(aa ? baseline.WithId($"{baselineVersion}-again") : Job.Default.WithId("Local"))
    .HideColumns(Column.Arguments); // it only repeats what the Job column says

BenchmarkSwitcher
    .FromTypes([typeof(CompositeReaderBenchmark), typeof(NumericAppenderBenchmark), typeof(HugeIntAppenderBenchmark), typeof(DateAppenderBenchmark), typeof(FunctionBenchmark), typeof(ListAppenderBenchmark), typeof(DecimalAppenderBenchmark), typeof(DecimalParameterBenchmark), typeof(StringAppenderBenchmark), typeof(RowCostBenchmark), typeof(TableFunctionRowBenchmark), typeof(EnumAppenderBenchmark), typeof(MappedAppenderBenchmark), typeof(DataReaderBenchmark), typeof(DateTimeReaderBenchmark), typeof(MixedNumericReaderBenchmark), typeof(StringReaderBenchmark), typeof(TypedReaderBenchmark), typeof(WideTableReaderBenchmark), typeof(CommandBenchmark)])
    .Run(args, config);
