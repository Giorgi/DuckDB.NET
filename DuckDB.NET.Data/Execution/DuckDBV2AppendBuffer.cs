using System.Linq;
using System.Text;
using DuckDB.NET.Data.Extensions;
using DuckDB.NET.Native.V2;

namespace DuckDB.NET.Data.Execution;

/// <summary>
/// Where <see cref="DuckDBAppender"/> puts its rows on the C API V2, which has no appender: a data chunk that the
/// writers fill, a column data collection that buffers the chunks, and INSERT statements that read the collection
/// (<c>statement_add_collection</c>) when it is flushed. See docs/V2-MIGRATION.md, "The appender".
/// <list type="bullet">
/// <item>The collection is flushed every <see cref="FlushThreshold"/> rows, as DuckDB's appender flushed, and on
/// close. A failed flush drops the rows since the last one.</item>
/// <item>An INSERT can leave a column to its default only for all its rows, so rows are grouped by which columns
/// they leave to their defaults (a pattern), in runs of consecutive rows. A hidden last column numbers the rows, and
/// a flush of several runs inserts each with its own range of numbers, so the rows keep their order. The common case,
/// one run with no defaults, is one INSERT of everything.</item>
/// </list>
/// </summary>
internal sealed unsafe class DuckDBV2AppendBuffer : IDisposable
{
    /// <summary>The rows buffered before a flush: DuckDB's appender flushed at the same count.</summary>
    public const ulong FlushThreshold = 204_800;

    private const string BufferName = "__duckdb_net_appender";
    private const string RowNumberColumn = "__duckdb_net_row";

    private readonly DuckDBV2Connection connection;
    private readonly string qualifiedTable;
    private readonly string[] columnNames;
    private readonly DuckDBV2LogicalType rowNumberType;
    private readonly DuckDBV2DataChunk chunk;
    private readonly DuckDBV2ColumnDataCollection collection;
    private DuckDBV2CollectionAppendState appendState;

    // Pattern 0 leaves no column to its default; each other one is the columns that a row leaves to their defaults.
    private readonly List<bool[]> patterns = [new bool[0]];
    private readonly Dictionary<string, int> patternIds = new() { [""] = 0 };

    // The prepared INSERT of each pattern, for a flush of one run (whole) and of several (by range of row numbers).
    private readonly Dictionary<(int Pattern, bool Ranged), (DuckDBV2SqlStatement Statement, DuckDBV2PreparedStatement Prepared)> inserts = [];

    private readonly List<(int Pattern, long Start, long Count)> runs = [];
    private long buffered;
    private bool disposed;

    public DuckDBV2AppendBuffer(DuckDBV2Connection connection, string? catalog, string? schema, string table)
    {
        this.connection = connection;
        qualifiedTable = string.Join(".", new[] { catalog, schema, table }.Where(part => !string.IsNullOrEmpty(part)).Select(Quote));

        using var connectionLease = connection.Acquire();
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_factory* factory;
        NativeMethodsV2.duckdb_v2_connection_get_factory(connectionLease.Pointer, &factory, &error).ThrowIfFailed(ref error);

        (columnNames, ColumnTypes) = ReadColumns(connection, qualifiedTable);
        rowNumberType = DuckDBV2Types.FromId((IntPtr)factory, DUCKDB_V2_LOGICAL_TYPE_ID.BIGINT);

        try
        {
            // The chunk and the collection have the table's columns and the row numbers.
            var leases = new DuckDBV2LogicalType[ColumnTypes.Length + 1];
            ColumnTypes.CopyTo(leases, 0);
            leases[^1] = rowNumberType;
            var types = stackalloc _duckdb_v2_logical_type*[leases.Length];
            for (var index = 0; index < leases.Length; index++)
            {
                types[index] = (_duckdb_v2_logical_type*)leases[index].DangerousGetHandle();
            }

            _duckdb_v2_data_chunk* chunkPointer;
            NativeMethodsV2.duckdb_v2_data_chunk_create(factory, types, (ulong)leases.Length, &chunkPointer, &error).ThrowIfFailed(ref error);
            chunk = new DuckDBV2DataChunk(chunkPointer);

            _duckdb_v2_column_data_collection* collectionPointer;
            NativeMethodsV2.duckdb_v2_column_data_collection_create(factory, types, (ulong)leases.Length, &collectionPointer, &error).ThrowIfFailed(ref error);
            collection = new DuckDBV2ColumnDataCollection(collectionPointer);

            appendState = CreateAppendState();

            // Prepared now, so a table the connection cannot insert into is reported when the appender is created.
            GetInsert(0, ranged: false);
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <summary>The types of the table's columns, in order; the buffer owns them.</summary>
    public DuckDBV2LogicalType[] ColumnTypes { get; }

    public int ColumnCount => ColumnTypes.Length;

    /// <summary>How many rows the chunk holds.</summary>
    public ulong Capacity
    {
        get
        {
            using var lease = chunk.Acquire();
            _duckdb_v2_error_info* error = null;
            ulong capacity;
            NativeMethodsV2.duckdb_v2_data_chunk_get_capacity(lease.Pointer, &capacity, &error).ThrowIfFailed(ref error);
            return capacity;
        }
    }

    /// <summary>Column <paramref name="index"/> of the chunk the writers fill. It changes after each chunk is buffered.</summary>
    public _duckdb_v2_vector* GetVector(int index) => DataChunk.Reader.DuckDBV2Vectors.GetVector(chunk, index);

    /// <summary>The id of the pattern of a row that leaves <paramref name="defaulted"/> to their defaults.</summary>
    public int GetPattern(bool[] defaulted)
    {
        var key = new StringBuilder();
        for (var index = 0; index < defaulted.Length; index++)
        {
            if (defaulted[index])
            {
                key.Append(index).Append(',');
            }
        }

        var text = key.ToString();
        if (!patternIds.TryGetValue(text, out var id))
        {
            id = patterns.Count;
            patterns.Add((bool[])defaulted.Clone());
            patternIds.Add(text, id);
        }

        return id;
    }

    /// <summary>
    /// Buffers the first <paramref name="rows"/> rows of the chunk, whose patterns are <paramref name="rowPatterns"/>,
    /// and empties the chunk. Flushes when <see cref="FlushThreshold"/> rows are buffered.
    /// </summary>
    public void BufferChunk(ulong rows, ReadOnlySpan<int> rowPatterns)
    {
        if (rows > 0)
        {
            // Every vector is told the number of rows; the row numbers go into the hidden last column.
            for (var index = 0; index <= ColumnCount; index++)
            {
                DataChunk.Reader.DuckDBV2Vectors.SetSize(GetVector(index), rows);
            }

            var numbers = (long*)DataChunk.Reader.DuckDBV2Vectors.GetMutableData(GetVector(ColumnCount));
            for (ulong row = 0; row < rows; row++)
            {
                numbers[row] = buffered + (long)row;
                AddToRun(rowPatterns[(int)row], buffered + (long)row);
            }

            using var collectionLease = collection.Acquire();
            using var stateLease = appendState.Acquire();
            using var chunkLease = chunk.Acquire();
            _duckdb_v2_error_info* error = null;
            NativeMethodsV2.duckdb_v2_column_data_collection_append(collectionLease.Pointer, stateLease.Pointer, chunkLease.Pointer, &error).ThrowIfFailed(ref error);

            buffered += (long)rows;
        }

        ClearChunk();

        if ((ulong)buffered >= FlushThreshold)
        {
            Flush();
        }
    }

    private void AddToRun(int pattern, long rowNumber)
    {
        if (runs.Count > 0 && runs[^1].Pattern == pattern)
        {
            var run = runs[^1];
            runs[^1] = (run.Pattern, run.Start, run.Count + 1);
        }
        else
        {
            runs.Add((pattern, rowNumber, 1));
        }
    }

    /// <summary>Inserts the buffered rows into the table. They are dropped either way, as a failed appender flush dropped them.</summary>
    public void Flush()
    {
        if (buffered == 0)
        {
            return;
        }

        try
        {
            var ranged = runs.Count > 1;
            foreach (var run in runs)
            {
                Insert(run.Pattern, ranged, run.Start, run.Count);
            }
        }
        finally
        {
            ClearBuffered();
        }
    }

    /// <summary>Drops the buffered rows and the chunk's, without inserting them.</summary>
    public void Clear()
    {
        ClearBuffered();
        ClearChunk();
    }

    // Clearing the collection invalidates its append state, so the state is made again.
    private void ClearBuffered()
    {
        runs.Clear();
        buffered = 0;

        appendState.Dispose();

        using (var lease = collection.Acquire())
        {
            _duckdb_v2_error_info* error = null;
            NativeMethodsV2.duckdb_v2_column_data_collection_clear(lease.Pointer, &error).ThrowIfFailed(ref error);
        }

        appendState = CreateAppendState();
    }

    private DuckDBV2CollectionAppendState CreateAppendState()
    {
        using var lease = collection.Acquire();
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_column_data_collection_append_state* state;
        NativeMethodsV2.duckdb_v2_column_data_collection_append_state_create(lease.Pointer, &state, &error).ThrowIfFailed(ref error);
        return new DuckDBV2CollectionAppendState(state);
    }

    private void ClearChunk()
    {
        using var lease = chunk.Acquire();
        _duckdb_v2_error_info* error = null;
        NativeMethodsV2.duckdb_v2_data_chunk_clear(lease.Pointer, &error).ThrowIfFailed(ref error);
    }

    private void Insert(int pattern, bool ranged, long start, long count)
    {
        // A row that leaves every column to its default has nothing to read from the buffer.
        if (pattern != 0 && Array.TrueForAll(patterns[pattern], defaulted => defaulted))
        {
            for (long row = 0; row < count; row++)
            {
                Execute(GetInsert(pattern, ranged: false).Prepared, null);
            }

            return;
        }

        var (_, prepared) = GetInsert(pattern, ranged);

        if (!ranged)
        {
            Execute(prepared, null);
            return;
        }

        var factory = GetFactory();
        using var from = DuckDBV2Values.Int64(factory, start);
        using var to = DuckDBV2Values.Int64(factory, start + count);
        Execute(prepared, [from, to]);
    }

    private void Execute(DuckDBV2PreparedStatement prepared, DuckDBV2Value[]? values)
    {
        using var lease = prepared.Acquire();
        using var scope = DuckDBV2CallbackScope.EnterExecution(connection);

        var count = values?.Length ?? 0;
        var names = stackalloc sbyte*[Math.Max(count, 1)];
        var lengths = stackalloc ulong[Math.Max(count, 1)];
        var pointers = stackalloc _duckdb_v2_value*[Math.Max(count, 1)];
        byte* positions = stackalloc byte[2] { (byte)'1', (byte)'2' };

        for (var index = 0; index < count; index++)
        {
            names[index] = (sbyte*)(positions + index);
            lengths[index] = 1;
            pointers[index] = (_duckdb_v2_value*)values![index].DangerousGetHandle();
        }

        _duckdb_v2_error_info* error = null;
        _duckdb_v2_result* result;
        var code = NativeMethodsV2.duckdb_v2_prepared_statement_execute(lease.Pointer, names, lengths, pointers, (ulong)count, &result, &error);

        using var statementResult = DuckDBV2Statements.Wrap(code, result, ref error, DuckDBStatementType.Insert, connection);
        statementResult.Finish();
    }

    private (DuckDBV2SqlStatement Statement, DuckDBV2PreparedStatement Prepared) GetInsert(int pattern, bool ranged)
    {
        if (inserts.TryGetValue((pattern, ranged), out var insert))
        {
            return insert;
        }

        var defaulted = patterns[pattern];
        var columns = string.Join(", ", columnNames.Where((_, index) => index >= defaulted.Length || !defaulted[index]).Select(Quote));

        var sql = columns.Length == 0
            ? $"INSERT INTO {qualifiedTable} DEFAULT VALUES"
            : $"INSERT INTO {qualifiedTable} ({columns}) SELECT {columns} FROM {BufferName}" +
              (ranged ? $" WHERE {Quote(RowNumberColumn)} >= $1 AND {Quote(RowNumberColumn)} < $2" : "");

        insert = Prepare(sql, readsBuffer: columns.Length > 0);
        inserts.Add((pattern, ranged), insert);
        return insert;
    }

    private (DuckDBV2SqlStatement, DuckDBV2PreparedStatement) Prepare(string sql, bool readsBuffer)
    {
        using var iterator = DuckDBV2Statements.Parse(connection, sql);
        var statement = DuckDBV2Statements.Next(iterator)!;

        try
        {
            using var statementLease = statement.Acquire();
            _duckdb_v2_error_info* error = null;

            if (readsBuffer)
            {
                AddBuffer(statementLease.Pointer);
            }

            using var connectionLease = connection.Acquire();
            using var scope = DuckDBV2CallbackScope.EnterExecution(connection);
            _duckdb_v2_prepared_statement* prepared;
            var code = NativeMethodsV2.duckdb_v2_prepared_statement_create(connectionLease.Pointer, statementLease.Pointer, 0, &prepared, &error);

            try
            {
                code.ThrowIfFailed(ref error, connection, "Preparing the insert of the appender failed");
            }
            catch (DuckDBException exception) when (exception.NativeErrorCode == DuckDBErrorCode.ResourceInUse)
            {
                throw DuckDBV2Statements.OpenReader(exception);
            }

            return (statement, new DuckDBV2PreparedStatement(prepared));
        }
        catch
        {
            statement.Dispose();
            throw;
        }
    }

    // The buffer is visible to the statement as a table of the table's column names and the row numbers.
    private void AddBuffer(_duckdb_v2_sql_statement* statement)
    {
        var names = new string[columnNames.Length + 1];
        columnNames.CopyTo(names, 0);
        names[^1] = RowNumberColumn;

        var buffers = Array.ConvertAll(names, name => Encoding.UTF8.GetBytes(name));
        var handles = buffers.Select(buffer => System.Runtime.InteropServices.GCHandle.Alloc(buffer, System.Runtime.InteropServices.GCHandleType.Pinned)).ToArray();

        try
        {
            var pointers = stackalloc sbyte*[names.Length];
            var lengths = stackalloc ulong[names.Length];
            for (var index = 0; index < names.Length; index++)
            {
                pointers[index] = (sbyte*)handles[index].AddrOfPinnedObject();
                lengths[index] = (ulong)buffers[index].Length;
            }

            var bufferName = Encoding.UTF8.GetBytes(BufferName);
            using var collectionLease = collection.Acquire();
            _duckdb_v2_error_info* error = null;

            fixed (byte* namePointer = bufferName)
            {
                NativeMethodsV2.duckdb_v2_statement_add_collection(statement, (sbyte*)namePointer, (ulong)bufferName.Length, collectionLease.Pointer, pointers, lengths, (ulong)names.Length, &error)
                               .ThrowIfFailed(ref error);
            }
        }
        finally
        {
            foreach (var handle in handles)
            {
                handle.Free();
            }
        }
    }

    // The table's columns, from binding SELECT * FROM it: their names and types.
    internal static (string[] Names, DuckDBV2LogicalType[] Types) ReadColumns(DuckDBV2Connection connection, string qualifiedTable)
    {
        using var iterator = DuckDBV2Statements.Parse(connection, $"SELECT * FROM {qualifiedTable}");
        using var statement = DuckDBV2Statements.Next(iterator)!;
        using var connectionLease = connection.Acquire();
        using var statementLease = statement.Acquire();

        _duckdb_v2_error_info* error = null;
        _duckdb_v2_schema* output;
        _duckdb_v2_schema* parameters;
        var code = NativeMethodsV2.duckdb_v2_statement_bind(connectionLease.Pointer, statementLease.Pointer, &output, &parameters, &error);

        try
        {
            code.ThrowIfFailed(ref error);
        }
        catch (DuckDBException exception) when (exception.NativeErrorCode == DuckDBErrorCode.ResourceInUse)
        {
            throw DuckDBV2Statements.OpenReader(exception);
        }

        NativeMethodsV2.duckdb_v2_schema_destroy(&parameters);

        using var schema = new DuckDBV2StatementResult.Schema(output);
        var names = new string[schema.Count];
        var types = new DuckDBV2LogicalType[schema.Count];

        for (var index = 0; index < schema.Count; index++)
        {
            var (name, type) = schema.GetField(index);
            names[index] = name;
            types[index] = DuckDBV2LogicalTypes.Copy((_duckdb_v2_logical_type*)type);
        }

        return (names, types);
    }

    private IntPtr GetFactory()
    {
        using var lease = connection.Acquire();
        _duckdb_v2_error_info* error = null;
        _duckdb_v2_factory* factory;
        NativeMethodsV2.duckdb_v2_connection_get_factory(lease.Pointer, &factory, &error).ThrowIfFailed(ref error);
        return (IntPtr)factory;
    }

    internal static string Quote(string? identifier) => "\"" + identifier!.Replace("\"", "\"\"") + "\"";

    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        disposed = true;

        // The statements read the collection, so they go first.
        foreach (var (statement, prepared) in inserts.Values)
        {
            prepared.Dispose();
            statement.Dispose();
        }

        appendState?.Dispose();
        collection?.Dispose();
        chunk?.Dispose();
        rowNumberType?.Dispose();

        foreach (var type in ColumnTypes ?? [])
        {
            type.Dispose();
        }
    }
}
