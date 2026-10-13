using DuckDB.NET.Data.DataChunk.Writer;
using System.Runtime.InteropServices;

namespace DuckDB.NET.Data.Connection;

// One scan of a table function: the rows of its data and the code that writes a row. The callback that DuckDB
// calls is static and cannot be generic, so it sees this class and calls Fill once for every chunk. The type of
// the rows is kept by the generic classes below, from the data to the code that writes a row, so a row is never
// passed as object: no cast for a row, and no box for a row that is a struct.
//
// A scan is made when DuckDB starts it, which is when the columns that the query reads are known. The writers
// that Fill is given are the writers of those columns only, in the order in which DuckDB asked for them.
internal abstract class TableFunctionScan : IDisposable
{
    /// <summary>
    /// Writes the next rows, row i of this call at position i, and returns how many were written.
    /// DuckDB calls again until a call writes no row.
    /// </summary>
    /// <param name="writers">The writers of the columns that the query reads, in the order of the chunk.</param>
    public abstract ulong Fill(VectorDataWriterBase[] writers, ulong maxRows);

    public virtual void Dispose()
    {
    }

    // An array and a List<T> are read by position, with no enumerator. Every other source is enumerated, and so
    // is a class derived from List<T>, which may enumerate in a way of its own.
    public static TableFunctionScan Create<T>(IEnumerable<T> rows, Action<T, VectorDataWriterBase[], ulong> write) => rows switch
    {
        T[] array => new ArrayScan<T>(array, write),
        List<T> list when list.GetType() == typeof(List<T>) => new ListScan<T>(list, write),
        _ => new EnumeratorScan<T>(rows.GetEnumerator(), write)
    };

    private sealed class ArrayScan<T>(T[] rows, Action<T, VectorDataWriterBase[], ulong> write) : TableFunctionScan
    {
        private int position;

        public override ulong Fill(VectorDataWriterBase[] writers, ulong maxRows)
        {
            var count = (int)Math.Min(maxRows, (ulong)(rows.Length - position));
            var chunk = new ReadOnlySpan<T>(rows, position, count);

            for (var index = 0; index < chunk.Length; index++)
            {
                write(chunk[index], writers, (ulong)index);
            }

            position += count;
            return (ulong)count;
        }
    }

    private sealed class ListScan<T>(List<T> rows, Action<T, VectorDataWriterBase[], ulong> write) : TableFunctionScan
    {
        private readonly int count = rows.Count;
        private int position;

        public override ulong Fill(VectorDataWriterBase[] writers, ulong maxRows)
        {
            ThrowIfChanged();

            // The span is taken for every chunk: it is not kept between two calls.
            var chunk = CollectionsMarshal.AsSpan(rows).Slice(position, (int)Math.Min(maxRows, (ulong)(count - position)));

            for (var index = 0; index < chunk.Length; index++)
            {
                write(chunk[index], writers, (ulong)index);
            }

            // After the chunk too: a query with a LIMIT may not ask for another one.
            ThrowIfChanged();

            position += chunk.Length;
            return (ulong)chunk.Length;
        }

        // The enumerator of a list refuses a list that was changed since it started. Read by position, a row
        // that was added or removed would shift the rows that follow without a word, so a list of another
        // length is refused, before a chunk and after it. A change that keeps the length is not noticed.
        private void ThrowIfChanged()
        {
            if (rows.Count != count)
            {
                throw new InvalidOperationException("Collection was modified; the rows of a table function cannot be added or removed while it is read.");
            }
        }
    }

    private sealed class EnumeratorScan<T>(IEnumerator<T> rows, Action<T, VectorDataWriterBase[], ulong> write) : TableFunctionScan
    {
        public override ulong Fill(VectorDataWriterBase[] writers, ulong maxRows)
        {
            ulong count = 0;

            while (count < maxRows && rows.MoveNext())
            {
                write(rows.Current, writers, count);
                count++;
            }

            return count;
        }

        public override void Dispose() => rows.Dispose();
    }
}

// The low-level registration: the rows are an IEnumerable, and the code that writes a row takes an object and a
// writer for every column of the function, at the index of the column. A column that the query does not read
// gets a writer that does nothing.
internal sealed class ObjectTableFunctionScan : TableFunctionScan
{
    private readonly IEnumerator rows;
    private readonly Action<object?, IDuckDBDataWriter[], ulong> write;
    private readonly int[] projected;
    private readonly IDuckDBDataWriter[] allWriters;

    public ObjectTableFunctionScan(IEnumerator rows, Action<object?, IDuckDBDataWriter[], ulong> write, int[] projected, int columnCount)
    {
        this.rows = rows;
        this.write = write;
        this.projected = projected;

        allWriters = new IDuckDBDataWriter[columnCount];
        Array.Fill(allWriters, NullDataWriter.Instance);
    }

    public override ulong Fill(VectorDataWriterBase[] writers, ulong maxRows)
    {
        for (var index = 0; index < projected.Length; index++)
        {
            allWriters[projected[index]] = writers[index];
        }

        ulong count = 0;

        while (count < maxRows && rows.MoveNext())
        {
            write(rows.Current, allWriters, count);
            count++;
        }

        return count;
    }

    public override void Dispose() => (rows as IDisposable)?.Dispose();
}
