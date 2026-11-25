using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace DuckDB.NET.Data;

/// <summary>
/// Appends Apache Arrow record batches to a DuckDB table.
/// </summary>
/// <remarks>
/// Not available on this branch: the appender has not been rebuilt on DuckDB's C API V2 yet, so
/// <see cref="DuckDBConnection.CreateArrowAppender(string)"/> throws <see cref="NotSupportedException"/> and no
/// instance can be created (docs/V2-MIGRATION.md). The members are kept so that code using them still compiles.
/// </remarks>
public sealed class DuckDBArrowAppender : IDisposable
{
    internal const string NotMigrated = "The Arrow appender is not available yet: this build of DuckDB.NET runs on DuckDB's C API V2, " +
                                        "and the Arrow appender has not been migrated to it (docs/V2-MIGRATION.md).";

    private DuckDBArrowAppender()
    {
    }

    /// <summary>
    /// Appends every row of <paramref name="batch"/>.
    /// </summary>
    public void AppendBatch(RecordBatch batch) => throw new NotSupportedException(NotMigrated);

    /// <summary>
    /// Reads <paramref name="stream"/> to the end and appends every batch.
    /// </summary>
    /// <returns>The number of rows appended.</returns>
    public long AppendStream(IArrowArrayStream stream) => throw new NotSupportedException(NotMigrated);

    /// <summary>
    /// Reads <paramref name="stream"/> to the end and appends every batch.
    /// </summary>
    /// <returns>The number of rows appended.</returns>
    public ValueTask<long> AppendStreamAsync(IArrowArrayStream stream, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException(NotMigrated);

    /// <summary>
    /// Drops the data appended since the appender last flushed.
    /// </summary>
    public void Clear() => throw new NotSupportedException(NotMigrated);

    public void Close() => throw new NotSupportedException(NotMigrated);

    public void Dispose()
    {
    }
}
