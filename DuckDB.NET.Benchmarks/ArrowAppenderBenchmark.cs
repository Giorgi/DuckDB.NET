#if ARROW_APPENDER
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;
using DuckDB.NET.Native;

namespace DuckDB.NET.Benchmarks;

// Appends 1M rows x 8 INTEGER columns as Arrow batches, like the other appender benchmarks, to measure the cost of the
// Arrow path itself. The batches are built once in GlobalSetup, so building them is not measured.
// - AppendBatches: DuckDBArrowAppender into INTEGER columns, the types match exactly.
// - AppendBatchesWithCast: the same int32 data into BIGINT columns, so DuckDB casts every column.
// - NativeNoSlicing: the same C API calls as AppendBatches, made directly and without slicing batches longer than
//   VectorSize. On batches of up to 2048 rows it shows the wrapper's overhead, on longer ones the cost of slicing.
//   It works only because the types match: DuckDB v2.0.0 cannot cast a chunk longer than 2048 rows.
// Only builds that have DuckDBArrowAppender can run it (ARROW_APPENDER, set in the project file), so every job of a
// run must be such a build.
[MemoryDiagnoser]
public class ArrowAppenderBenchmark
{
    private const int ColumnCount = 8;

    private DuckDBConnection connection = null!;
    private RecordBatch[] batches = null!;

    [Params(1_000_000)]
    public int RowCount { get; set; }

    [Params(256, 2048, 65536)]
    public int BatchRows { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();

        LoadedLibrary.Print(connection);

        var schemaBuilder = new Schema.Builder();
        for (var column = 0; column < ColumnCount; column++)
        {
            var name = $"c{column}";
            schemaBuilder.Field(f => f.Name(name).DataType(Int32Type.Default));
        }

        var schema = schemaBuilder.Build();
        var list = new List<RecordBatch>();

        for (var start = 0; start < RowCount; start += BatchRows)
        {
            var length = Math.Min(BatchRows, RowCount - start);
            var arrays = new IArrowArray[ColumnCount];

            for (var column = 0; column < ColumnCount; column++)
            {
                var builder = new Int32Array.Builder().Reserve(length);
                for (var row = 0; row < length; row++)
                {
                    builder.Append(start + row + column);
                }

                arrays[column] = builder.Build();
            }

            list.Add(new RecordBatch(schema, arrays, length));
        }

        batches = list.ToArray();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        connection.Dispose();
    }

    [IterationSetup]
    public void CreateTables()
    {
        var integers = string.Join(", ", Enumerable.Range(0, ColumnCount).Select(c => $"c{c} INTEGER"));
        var bigints = string.Join(", ", Enumerable.Range(0, ColumnCount).Select(c => $"c{c} BIGINT"));

        using var command = connection.CreateCommand();
        command.CommandText = $"DROP TABLE IF EXISTS bench_int; DROP TABLE IF EXISTS bench_bigint; CREATE TABLE bench_int ({integers}); CREATE TABLE bench_bigint ({bigints});";
        command.ExecuteNonQuery();
    }

    [Benchmark]
    public void AppendBatches()
    {
        using var appender = connection.CreateArrowAppender("bench_int");

        foreach (var batch in batches)
        {
            appender.AppendBatch(batch);
        }
    }

    [Benchmark]
    public void AppendBatchesWithCast()
    {
        using var appender = connection.CreateArrowAppender("bench_bigint");

        foreach (var batch in batches)
        {
            appender.AppendBatch(batch);
        }
    }

    [Benchmark(Baseline = true)]
    public unsafe void NativeNoSlicing()
    {
        var nativeConnection = connection.NativeConnection;

        Check(NativeMethods.Appender.DuckDBAppenderCreateExt(nativeConnection, null, null, "bench_int", out var appender), appender);

        using (appender)
        {
            var cSchema = CArrowSchema.Create();
            DuckDBArrowConvertedSchema convertedSchema;

            try
            {
                CArrowSchemaExporter.ExportSchema(batches[0].Schema, cSchema);
                using var schemaError = NativeMethods.Arrow.DuckDBSchemaFromArrow(nativeConnection, (IntPtr)cSchema, out convertedSchema);
                ThrowIfError(schemaError);
            }
            finally
            {
                CArrowSchema.Free(cSchema);
            }

            using (convertedSchema)
            {
                foreach (var batch in batches)
                {
                    var cArray = CArrowArray.Create();

                    try
                    {
                        CArrowArrayExporter.ExportRecordBatch(batch, cArray);

                        using var error = NativeMethods.Arrow.DuckDBDataChunkFromArrow(nativeConnection, (IntPtr)cArray, convertedSchema, out var chunk);
                        using (chunk)
                        {
                            ThrowIfError(error);
                            Check(NativeMethods.Appender.DuckDBAppendDataChunk(appender, chunk), appender);
                        }
                    }
                    finally
                    {
                        CArrowArray.Free(cArray);
                    }
                }
            }

            Check(NativeMethods.Appender.DuckDBAppenderClose(appender), appender);
        }
    }

    private static void Check(DuckDBState state, Native.DuckDBAppender appender)
    {
        if (state != DuckDBState.Success)
        {
            using var error = NativeMethods.Appender.DuckDBAppenderErrorData(appender);
            throw new InvalidOperationException(error.Message);
        }
    }

    private static void ThrowIfError(DuckDBErrorData error)
    {
        if (error.HasError)
        {
            throw new InvalidOperationException(error.Message);
        }
    }
}
#endif
