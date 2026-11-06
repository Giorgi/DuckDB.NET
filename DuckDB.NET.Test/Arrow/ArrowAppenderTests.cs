using System.Threading;
using Apache.Arrow;
using Apache.Arrow.Types;
using DuckDB.NET.Data.Extensions;

namespace DuckDB.NET.Test.Arrow;

public class ArrowAppenderTests(DuckDBDatabaseFixture db) : DuckDBTestBase(db)
{
    [Fact]
    public void AppendBatch_WritesValuesAndNulls()
    {
        Connection.ExecuteNonQuery("CREATE TABLE arrow_values(i INTEGER, s VARCHAR, d DOUBLE, b BOOLEAN, ts TIMESTAMP, m DECIMAL(18, 3))");

        var schema = new Schema.Builder()
            .Field(f => f.Name("i").DataType(Int32Type.Default))
            .Field(f => f.Name("s").DataType(StringType.Default))
            .Field(f => f.Name("d").DataType(DoubleType.Default))
            .Field(f => f.Name("b").DataType(BooleanType.Default))
            .Field(f => f.Name("ts").DataType(new TimestampType(TimeUnit.Microsecond, (string)null)))
            .Field(f => f.Name("m").DataType(new Decimal128Type(18, 3)))
            .Build();

        var timestamp = new DateTimeOffset(2026, 11, 6, 12, 30, 15, TimeSpan.Zero);

        var batch = new RecordBatch(schema,
        [
            new Int32Array.Builder().Append(1).AppendNull().Append(3).Build(),
            new StringArray.Builder().Append("one").Append("two").AppendNull().Build(),
            new DoubleArray.Builder().AppendNull().Append(2.5).Append(3.5).Build(),
            new BooleanArray.Builder().Append(true).Append(false).AppendNull().Build(),
            new TimestampArray.Builder(TimeUnit.Microsecond, (string)null).Append(timestamp).AppendNull().Append(timestamp.AddSeconds(1)).Build(),
            new Decimal128Array.Builder(new Decimal128Type(18, 3)).Append(1.234m).Append(-5.5m).AppendNull().Build(),
        ], 3);

        using (var appender = Connection.CreateArrowAppender("arrow_values"))
        {
            appender.AppendBatch(batch);
        }

        var rows = Connection.Query<(int? I, string S, double? D, bool? B, DateTime? Ts, decimal? M)>(
            "SELECT i, s, d, b, ts, m FROM arrow_values ORDER BY rowid").ToList();

        rows.Should().Equal(
            (1, "one", null, true, timestamp.UtcDateTime, 1.234m),
            (null, "two", 2.5, false, null, -5.5m),
            (3, null, 3.5, null, timestamp.AddSeconds(1).UtcDateTime, null));
    }

    [Theory]
    [InlineData(2048)]
    [InlineData(2049)]
    [InlineData(5000)]
    public void AppendBatch_CastsColumnsInLongBatches(int rowCount)
    {
        // An int32 column into BIGINT needs a cast. DuckDB 2.0.0 cannot cast a chunk longer than 2048 rows
        // (duckdb/duckdb#27664), and the appender sends the whole batch as one chunk, so this needs a fixed engine.
        var table = $"arrow_cast_{rowCount}";
        Connection.ExecuteNonQuery($"CREATE TABLE {table}(exact INTEGER, widened BIGINT)");

        var schema = new Schema.Builder()
            .Field(f => f.Name("exact").DataType(Int32Type.Default))
            .Field(f => f.Name("widened").DataType(Int32Type.Default))
            .Build();

        var exact = new Int32Array.Builder();
        var widened = new Int32Array.Builder();
        for (var i = 0; i < rowCount; i++)
        {
            exact.Append(i);
            widened.Append(i);
        }

        using (var appender = Connection.CreateArrowAppender(table))
        {
            appender.AppendBatch(new RecordBatch(schema, [exact.Build(), widened.Build()], rowCount));
        }

        var result = Connection.QuerySingle<(long Count, long ExactSum, long WidenedSum, long OutOfPlace)>(
            $"SELECT count(*), sum(exact)::BIGINT, sum(widened)::BIGINT, count(*) FILTER (WHERE exact <> rowid OR widened <> rowid) FROM {table}");

        var expectedSum = (long)rowCount * (rowCount - 1) / 2;
        result.Should().Be((rowCount, expectedSum, expectedSum, 0L));
    }

    [Fact]
    public void AppendBatch_ReadsSlicedBatchFromItsOffset()
    {
        Connection.ExecuteNonQuery("CREATE TABLE arrow_sliced(i INTEGER, s VARCHAR, l INTEGER[])");

        var schema = new Schema.Builder()
            .Field(f => f.Name("i").DataType(Int32Type.Default))
            .Field(f => f.Name("s").DataType(StringType.Default))
            .Field(f => f.Name("l").DataType(new ListType(Int32Type.Default)))
            .Build();

        var ints = new Int32Array.Builder();
        var strings = new StringArray.Builder();
        var lists = new ListArray.Builder(Int32Type.Default);
        var listValues = (Int32Array.Builder)lists.ValueBuilder;

        for (var i = 0; i < 100; i++)
        {
            if (i % 4 == 0) ints.AppendNull(); else ints.Append(i);
            strings.Append("s" + i);
            lists.Append();
            listValues.Append(i);
            listValues.Append(i + 1);
        }

        var batch = new RecordBatch(schema, [ints.Build(), strings.Build(), lists.Build()], 100).Slice(10, 5);

        using (var appender = Connection.CreateArrowAppender("arrow_sliced"))
        {
            appender.AppendBatch(batch);
        }

        var rows = Connection.Query<(int? I, string S, int Second)>("SELECT i, s, l[2] FROM arrow_sliced ORDER BY rowid").ToList();

        rows.Should().Equal((10, "s10", 11), (11, "s11", 12), (null, "s12", 13), (13, "s13", 14), (14, "s14", 15));
    }

    [Fact]
    public void AppendBatch_WritesStructAndDictionaryColumns()
    {
        Connection.ExecuteNonQuery("CREATE TABLE arrow_nested(st STRUCT(a INTEGER, b VARCHAR), category VARCHAR)");

        var structType = new StructType(
        [
            new Field("a", Int32Type.Default, true),
            new Field("b", StringType.Default, true),
        ]);
        var structArray = new StructArray(structType, 2,
        [
            new Int32Array.Builder().Append(1).Append(2).Build(),
            new StringArray.Builder().Append("x").Append("y").Build(),
        ], ArrowBuffer.Empty, 0);

        var dictionaryType = new DictionaryType(Int32Type.Default, StringType.Default, false);
        var dictionary = new DictionaryArray(dictionaryType,
            new Int32Array.Builder().Append(1).Append(0).Build(),
            new StringArray.Builder().Append("red").Append("green").Build());

        var schema = new Schema.Builder()
            .Field(f => f.Name("st").DataType(structType))
            .Field(f => f.Name("category").DataType(dictionaryType))
            .Build();

        using (var appender = Connection.CreateArrowAppender("arrow_nested"))
        {
            appender.AppendBatch(new RecordBatch(schema, [structArray, dictionary], 2));
        }

        var rows = Connection.Query<(int A, string B, string Category)>("SELECT st.a, st.b, category FROM arrow_nested ORDER BY rowid").ToList();

        rows.Should().Equal((1, "x", "green"), (2, "y", "red"));
    }

    [Fact]
    public void AppendBatch_ConvertsAgainWhenSchemaChanges()
    {
        Connection.ExecuteNonQuery("CREATE TABLE arrow_schema_change(v BIGINT)");

        var intSchema = new Schema.Builder().Field(f => f.Name("v").DataType(Int32Type.Default)).Build();
        var longSchema = new Schema.Builder().Field(f => f.Name("v").DataType(Int64Type.Default)).Build();

        using (var appender = Connection.CreateArrowAppender("arrow_schema_change"))
        {
            appender.AppendBatch(new RecordBatch(intSchema, [new Int32Array.Builder().Append(1).Build()], 1));
            appender.AppendBatch(new RecordBatch(longSchema, [new Int64Array.Builder().Append(long.MaxValue).Build()], 1));
            appender.AppendBatch(new RecordBatch(intSchema, [new Int32Array.Builder().Append(3).Build()], 1));
        }

        Connection.Query<long>("SELECT v FROM arrow_schema_change ORDER BY rowid").Should().Equal(1L, long.MaxValue, 3L);
    }

    [Fact]
    public void AppendBatch_EmptyBatchAppendsNothing()
    {
        Connection.ExecuteNonQuery("CREATE TABLE arrow_empty(i INTEGER)");

        var schema = new Schema.Builder().Field(f => f.Name("i").DataType(Int32Type.Default)).Build();

        using (var appender = Connection.CreateArrowAppender("arrow_empty"))
        {
            appender.AppendBatch(new RecordBatch(schema, [new Int32Array.Builder().Build()], 0));
            appender.AppendBatch(new RecordBatch(schema, [new Int32Array.Builder().Append(7).Build()], 1));
        }

        Connection.Query<int>("SELECT i FROM arrow_empty").Should().Equal(7);
    }

    [Fact]
    public void AppendBatch_WrongColumnCountThrowsAndKeepsAppenderUsable()
    {
        Connection.ExecuteNonQuery("CREATE TABLE arrow_column_count(i INTEGER)");

        var twoColumns = new Schema.Builder()
            .Field(f => f.Name("i").DataType(Int32Type.Default))
            .Field(f => f.Name("j").DataType(Int32Type.Default))
            .Build();
        var oneColumn = new Schema.Builder().Field(f => f.Name("i").DataType(Int32Type.Default)).Build();

        using (var appender = Connection.CreateArrowAppender("arrow_column_count"))
        {
            var wrong = new RecordBatch(twoColumns, [new Int32Array.Builder().Append(1).Build(), new Int32Array.Builder().Append(2).Build()], 1);

            appender.Invoking(a => a.AppendBatch(wrong)).Should().Throw<ArgumentException>().WithMessage("*2 columns*1*");

            appender.AppendBatch(new RecordBatch(oneColumn, [new Int32Array.Builder().Append(5).Build()], 1));
        }

        Connection.Query<int>("SELECT i FROM arrow_column_count").Should().Equal(5);
    }

    [Fact]
    public void AppendBatch_FailedCastFaultsAppenderAndKeepsEarlierRows()
    {
        Connection.ExecuteNonQuery("CREATE TABLE arrow_failed_cast(i INTEGER)");

        var intSchema = new Schema.Builder().Field(f => f.Name("i").DataType(Int32Type.Default)).Build();
        var stringSchema = new Schema.Builder().Field(f => f.Name("i").DataType(StringType.Default)).Build();

        using (var appender = Connection.CreateArrowAppender("arrow_failed_cast"))
        {
            appender.AppendBatch(new RecordBatch(intSchema, [new Int32Array.Builder().Append(1).Append(2).Build()], 2));

            var bad = new RecordBatch(stringSchema, [new StringArray.Builder().Append("not a number").Build()], 1);
            appender.Invoking(a => a.AppendBatch(bad)).Should().Throw<DuckDBException>();

            var good = new RecordBatch(intSchema, [new Int32Array.Builder().Append(3).Build()], 1);
            appender.Invoking(a => a.AppendBatch(good)).Should().Throw<InvalidOperationException>().WithMessage("*batch failed*");
            appender.Invoking(a => a.Clear()).Should().Throw<InvalidOperationException>();
        }

        Connection.Query<int>("SELECT i FROM arrow_failed_cast ORDER BY rowid").Should().Equal(1, 2);
    }

    [Fact]
    public void Clear_DropsAppendedBatches()
    {
        Connection.ExecuteNonQuery("CREATE TABLE arrow_clear(i INTEGER)");

        var schema = new Schema.Builder().Field(f => f.Name("i").DataType(Int32Type.Default)).Build();

        using (var appender = Connection.CreateArrowAppender("arrow_clear"))
        {
            appender.AppendBatch(new RecordBatch(schema, [new Int32Array.Builder().Append(1).Build()], 1));
            appender.Clear();
            appender.AppendBatch(new RecordBatch(schema, [new Int32Array.Builder().Append(2).Build()], 1));
        }

        Connection.Query<int>("SELECT i FROM arrow_clear").Should().Equal(2);
    }

    [Fact]
    public void ClosedAppenderRejectsCalls()
    {
        Connection.ExecuteNonQuery("CREATE TABLE arrow_closed(i INTEGER)");

        var schema = new Schema.Builder().Field(f => f.Name("i").DataType(Int32Type.Default)).Build();
        var batch = new RecordBatch(schema, [new Int32Array.Builder().Append(1).Build()], 1);

        var appender = Connection.CreateArrowAppender("arrow_closed");
        appender.Close();

        appender.Invoking(a => a.AppendBatch(batch)).Should().Throw<InvalidOperationException>().WithMessage("*closed*");
        appender.Invoking(a => a.Close()).Should().Throw<InvalidOperationException>().WithMessage("*closed*");
        appender.Invoking(a => a.Dispose()).Should().NotThrow();
    }

    [Fact]
    public void CreateArrowAppender_ReportsMissingTableAndClosedConnection()
    {
        Connection.Invoking(c => c.CreateArrowAppender("arrow_missing_table")).Should().Throw<DuckDBException>();

        using var closed = new DuckDBConnection("DataSource=:memory:");
        closed.Invoking(c => c.CreateArrowAppender("anything")).Should().Throw<InvalidOperationException>()
            .WithMessage("CreateArrowAppender requires an open connection");
    }

    [Fact]
    public void CreateArrowAppender_UsesSchemaName()
    {
        Connection.ExecuteNonQuery("CREATE SCHEMA IF NOT EXISTS arrow_schema");
        Connection.ExecuteNonQuery("CREATE TABLE arrow_schema.target(i INTEGER)");

        var schema = new Schema.Builder().Field(f => f.Name("i").DataType(Int32Type.Default)).Build();

        using (var appender = Connection.CreateArrowAppender("arrow_schema", "target"))
        {
            appender.AppendBatch(new RecordBatch(schema, [new Int32Array.Builder().Append(9).Build()], 1));
        }

        Connection.Query<int>("SELECT i FROM arrow_schema.target").Should().Equal(9);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AppendStream_CopiesQueryResultFromAnotherDatabase(bool useAsync)
    {
        const int rowCount = 5000;

        using var source = new DuckDBConnection("DataSource=:memory:");
        source.Open();
        source.ExecuteNonQuery($"CREATE TABLE source AS SELECT i::INTEGER AS i, CASE WHEN i % 7 = 0 THEN NULL ELSE 'v' || i END AS s, [i, i + 1] AS l FROM range({rowCount}) t(i)");

        var table = useAsync ? "arrow_copy_async" : "arrow_copy";
        Connection.ExecuteNonQuery($"CREATE TABLE {table}(i BIGINT, s VARCHAR, l BIGINT[])");

        using var command = source.CreateCommand();
        command.CommandText = "SELECT i, s, l FROM source ORDER BY i";

        long appended;
        using (var stream = command.ExecuteArrowStream())
        using (var appender = Connection.CreateArrowAppender(table))
        {
            appended = useAsync
                ? await appender.AppendStreamAsync(stream, CancellationToken.None)
                : appender.AppendStream(stream);
        }

        appended.Should().Be(rowCount);

        var result = Connection.QuerySingle<(long Count, long Sum, long Nulls, long Mismatches)>(
            $"SELECT count(*), sum(i)::BIGINT, count(*) FILTER (WHERE s IS NULL), count(*) FILTER (WHERE i <> rowid OR s <> 'v' || i OR l <> [i, i + 1]) FROM {table}");

        result.Should().Be((rowCount, (long)rowCount * (rowCount - 1) / 2, (long)(rowCount + 6) / 7, 0L));
    }

    [Fact]
    public async Task AppendStreamAsync_StopsWhenCancelledAndKeepsEarlierBatches()
    {
        Connection.ExecuteNonQuery("CREATE TABLE arrow_cancel(i INTEGER)");

        var schema = new Schema.Builder().Field(f => f.Name("i").DataType(Int32Type.Default)).Build();
        using var cancellation = new CancellationTokenSource();

        // The token is cancelled while the appender reads the second batch, after it appended the first one.
        var stream = new ListStream(schema,
        [
            new RecordBatch(schema, [new Int32Array.Builder().Append(1).Build()], 1),
            new RecordBatch(schema, [new Int32Array.Builder().Append(2).Build()], 1),
        ], beforeRead: index => { if (index == 1) cancellation.Cancel(); });

        using (var appender = Connection.CreateArrowAppender("arrow_cancel"))
        {
            await appender.Invoking(a => a.AppendStreamAsync(stream, cancellation.Token).AsTask())
                .Should().ThrowAsync<OperationCanceledException>();
        }

        Connection.Query<int>("SELECT i FROM arrow_cancel").Should().Equal(1);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AppendBatch_ColumnShorterThanBatchThrows(bool strings)
    {
        // DuckDB 2.0.0 reads such a column past its end: garbage for integers, a crash for strings.
        Connection.ExecuteNonQuery($"CREATE TABLE arrow_short_{strings}(v {(strings ? "VARCHAR" : "INTEGER")})");

        var type = strings ? (IArrowType)StringType.Default : Int32Type.Default;
        var schema = new Schema.Builder().Field(f => f.Name("v").DataType(type)).Build();
        IArrowArray column = strings ? new StringArray.Builder().Append("x").Build() : new Int32Array.Builder().Append(1).Build();

        using (var appender = Connection.CreateArrowAppender($"arrow_short_{strings}"))
        {
            var batch = new RecordBatch(schema, [column], 100_000);
            appender.Invoking(a => a.AppendBatch(batch)).Should().Throw<ArgumentException>().WithMessage("Column 0 has 1 rows but the batch has 100000*");

            // Nothing was written, so the appender is still usable.
            appender.AppendBatch(new RecordBatch(schema, [column], 1));
        }

        Connection.QuerySingle<long>($"SELECT count(*) FROM arrow_short_{strings}").Should().Be(1);
    }

    [Fact]
    public void AppendBatch_LongMixedBatchFromOffset()
    {
        // Strings, booleans and NULLs across 2048-row vector boundaries, with a cast, from a batch that starts at an
        // offset.
        const int rowCount = 5003;
        Connection.ExecuteNonQuery("CREATE TABLE arrow_long_mixed(i BIGINT, s VARCHAR, b BOOLEAN)");

        var schema = new Schema.Builder()
            .Field(f => f.Name("i").DataType(Int32Type.Default))
            .Field(f => f.Name("s").DataType(StringType.Default))
            .Field(f => f.Name("b").DataType(BooleanType.Default))
            .Build();

        var ints = new Int32Array.Builder();
        var strings = new StringArray.Builder();
        var booleans = new BooleanArray.Builder();

        // One extra leading row, cut off by the slice below.
        for (var i = -1; i < rowCount; i++)
        {
            if (i % 5 == 0) ints.AppendNull(); else ints.Append(i);
            if (i % 3 == 0) strings.AppendNull(); else strings.Append("s" + i);
            if (i % 7 == 0) booleans.AppendNull(); else booleans.Append(i % 2 == 0);
        }

        var batch = new RecordBatch(schema, [ints.Build(), strings.Build(), booleans.Build()], rowCount + 1).Slice(1, rowCount);

        using (var appender = Connection.CreateArrowAppender("arrow_long_mixed"))
        {
            appender.AppendBatch(batch);
        }

        var mismatches = Connection.QuerySingle<(long Count, long Wrong)>("""
            SELECT count(*), count(*) FILTER (WHERE
                   (i IS DISTINCT FROM CASE WHEN rowid % 5 = 0 THEN NULL ELSE rowid END)
                OR (s IS DISTINCT FROM CASE WHEN rowid % 3 = 0 THEN NULL ELSE 's' || rowid END)
                OR (b IS DISTINCT FROM CASE WHEN rowid % 7 = 0 THEN NULL ELSE rowid % 2 = 0 END))
            FROM arrow_long_mixed
            """);

        mismatches.Should().Be((rowCount, 0L));
    }

    [Fact]
    public void AppendBatch_FailingLongBatchAppendsNothingOfIt()
    {
        // The batch goes to DuckDB as one chunk, so a value that fails to cast after the first 2048 rows leaves none
        // of the batch behind. The batch appended before it is still written.
        Connection.ExecuteNonQuery("CREATE TABLE arrow_partial(i INTEGER)");

        var schema = new Schema.Builder().Field(f => f.Name("i").DataType(StringType.Default)).Build();
        var values = new StringArray.Builder();
        for (var i = 0; i < 5000; i++)
        {
            values.Append(i == 3000 ? "not a number" : i.ToString());
        }

        using (var appender = Connection.CreateArrowAppender("arrow_partial"))
        {
            appender.AppendBatch(new RecordBatch(schema, [new StringArray.Builder().Append("-1").Append("-2").Build()], 2));

            var batch = new RecordBatch(schema, [values.Build()], 5000);
            appender.Invoking(a => a.AppendBatch(batch)).Should().Throw<DuckDBException>();
        }

        Connection.Query<int>("SELECT i FROM arrow_partial ORDER BY rowid").Should().Equal(-1, -2);
    }

    [Fact]
    public void AppendBatch_ConstraintFailureAtFlushDropsEarlierBatches()
    {
        // Constraints are checked when the appender flushes. A batch over the flush threshold (204,800 rows) makes
        // AppendBatch flush, and the failed flush drops the earlier, valid batch as well.
        Connection.ExecuteNonQuery("CREATE TABLE arrow_not_null(i INTEGER NOT NULL)");

        var schema = new Schema.Builder().Field(f => f.Name("i").DataType(Int32Type.Default)).Build();
        var valid = new Int32Array.Builder();
        for (var i = 0; i < 10; i++) valid.Append(i);

        var withNull = new Int32Array.Builder();
        for (var i = 0; i < 300_000; i++)
        {
            if (i == 150_000) withNull.AppendNull(); else withNull.Append(i);
        }

        using (var appender = Connection.CreateArrowAppender("arrow_not_null"))
        {
            appender.AppendBatch(new RecordBatch(schema, [valid.Build()], 10));

            var bad = new RecordBatch(schema, [withNull.Build()], 300_000);
            appender.Invoking(a => a.AppendBatch(bad)).Should().Throw<DuckDBException>().WithMessage("*NOT NULL*");
        }

        Connection.QuerySingle<long>("SELECT count(*) FROM arrow_not_null").Should().Be(0);
    }

    [Fact]
    public void Dispose_ConstraintFailureDropsUnflushedBatches()
    {
        // Below the flush threshold nothing is checked until the appender is closed: both batches append, and the
        // duplicate key fails at Dispose, which drops both.
        Connection.ExecuteNonQuery("CREATE TABLE arrow_primary_key(i INTEGER PRIMARY KEY)");

        var schema = new Schema.Builder().Field(f => f.Name("i").DataType(Int32Type.Default)).Build();
        var appender = Connection.CreateArrowAppender("arrow_primary_key");

        appender.AppendBatch(new RecordBatch(schema, [new Int32Array.Builder().Append(1).Append(2).Build()], 2));
        appender.AppendBatch(new RecordBatch(schema, [new Int32Array.Builder().Append(3).Append(1).Build()], 2));

        appender.Invoking(a => a.Dispose()).Should().Throw<DuckDBException>();
        appender.Invoking(a => a.Dispose()).Should().NotThrow();

        Connection.QuerySingle<long>("SELECT count(*) FROM arrow_primary_key").Should().Be(0);
    }

    private sealed class ListStream(Schema schema, IReadOnlyList<RecordBatch> batches, Action<int> beforeRead) : Apache.Arrow.Ipc.IArrowArrayStream
    {
        private int next;

        public Schema Schema => schema;

        public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            beforeRead(next);
            cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<RecordBatch>(next < batches.Count ? batches[next++] : null);
        }

        public void Dispose()
        {
        }
    }
}
