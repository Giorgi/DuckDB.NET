using System;
using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace DuckDB.NET.Test.Arrow;

public class ArrowResultTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExecuteArrowBatches_ReturnsSchemaAndScalarValues(bool useStreamingMode)
    {
        Command.UseStreamingMode = useStreamingMode;
        Command.CommandText = "select 42 as answer, 'duckdb' as name, cast(3.5 as double) as ratio";

        var batches = await ReadAllAsync();

        batches.Should().ContainSingle();

        var batch = batches[0];
        batch.Schema.FieldsList.Select(f => f.Name).Should().Equal("answer", "name", "ratio");
        batch.Length.Should().Be(1);

        ((Int32Array)batch.Column("answer")).GetValue(0).Should().Be(42);
        ((StringArray)batch.Column("name")).GetString(0).Should().Be("duckdb");
        ((DoubleArray)batch.Column("ratio")).GetValue(0).Should().Be(3.5);

        DisposeBatches(batches);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExecuteArrowBatches_HandlesNullValues(bool useStreamingMode)
    {
        Command.UseStreamingMode = useStreamingMode;
        Command.CommandText = "select unnest([1, null, 3]) as value";

        var batches = await ReadAllAsync();

        var column = (Int32Array)batches.Single().Column("value");
        column.Length.Should().Be(3);
        column.GetValue(0).Should().Be(1);
        column.GetValue(1).Should().BeNull();
        column.GetValue(2).Should().Be(3);

        DisposeBatches(batches);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExecuteArrowBatches_StreamsMultipleChunks(bool useStreamingMode)
    {
        const int rowCount = 5000;
        Command.UseStreamingMode = useStreamingMode;
        Command.CommandText = $"select i from range({rowCount}) t(i)";

        var batches = await ReadAllAsync();

        batches.Count.Should().BeGreaterThan(1);
        batches.Sum(b => b.Length).Should().Be(rowCount);

        var total = 0L;
        foreach (var batch in batches)
        {
            var column = (Int64Array)batch.Column("i");
            for (var row = 0; row < column.Length; row++)
            {
                total += column.GetValue(row)!.Value;
            }
        }

        total.Should().Be((long)rowCount * (rowCount - 1) / 2);

        DisposeBatches(batches);
    }

    [Fact]
    public void ExecuteArrowStream_ExposesSchemaWithoutReading()
    {
        Command.CommandText = "select 1 as a, 'x' as b";

        using var stream = Command.ExecuteArrowStream();

        stream.Schema.FieldsList.Select(f => f.Name).Should().Equal("a", "b");
        stream.Schema.GetFieldByName("a").DataType.TypeId.Should().Be(ArrowTypeId.Int32);
        stream.Schema.GetFieldByName("b").DataType.TypeId.Should().Be(ArrowTypeId.String);
    }

    [Fact]
    public async Task ExecuteArrowStream_ReadsBatchesUntilNull()
    {
        Command.CommandText = "select i from range(10) t(i)";

        using var stream = Command.ExecuteArrowStream();

        var rows = 0;
        while (await stream.ReadNextRecordBatchAsync(CancellationToken.None) is { } batch)
        {
            rows += batch.Length;
            batch.Dispose();
        }

        rows.Should().Be(10);
    }

    [Fact]
    public void ExecuteArrowStream_ThrowsWhenNoResultSet()
    {
        Command.CommandText = "create table t_no_result (id integer)";

        var act = () => Command.ExecuteArrowStream();

        act.Should().Throw<InvalidOperationException>();
    }

    [Fact]
    public async Task ReadNextRecordBatchAsync_WithCanceledToken_ReturnsCanceledTask()
    {
        Command.CommandText = "select i from range(10) t(i)";

        using var stream = Command.ExecuteArrowStream();

        var act = async () => await stream.ReadNextRecordBatchAsync(new CancellationToken(canceled: true));

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task ReadNextRecordBatchAsync_AfterDispose_Throws()
    {
        Command.CommandText = "select i from range(10) t(i)";

        var stream = Command.ExecuteArrowStream();
        stream.Dispose();

        var act = async () => await stream.ReadNextRecordBatchAsync(CancellationToken.None);

        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    [Fact]
    public void Dispose_CalledTwice_IsNoOp()
    {
        Command.CommandText = "select i from range(10) t(i)";

        var stream = Command.ExecuteArrowStream();

        stream.Dispose();
        var act = () => stream.Dispose();

        act.Should().NotThrow();
    }

    private async Task<List<RecordBatch>> ReadAllAsync()
    {
        var batches = new List<RecordBatch>();
        await foreach (var batch in Command.ExecuteArrowBatchesAsync())
        {
            batches.Add(batch);
        }

        return batches;
    }

    private static void DisposeBatches(List<RecordBatch> batches)
    {
        foreach (var batch in batches)
        {
            batch.Dispose();
        }
    }
}
