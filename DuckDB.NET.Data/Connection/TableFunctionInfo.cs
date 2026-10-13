using DuckDB.NET.Data.DataChunk.Writer;

namespace DuckDB.NET.Data.Connection;

class TableFunctionInfo(Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> bind, TableFunctionScanFactory createScan, string[] namedParameterNames)
{
    public Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> Bind { get; } = bind;

    // Makes the scan of the data that Bind returned. It knows the type of the rows; nothing else here does.
    public TableFunctionScanFactory CreateScan { get; } = createScan;
    public string[] NamedParameterNames { get; } = namedParameterNames;
}

record NamedParameterDefinition(string Name, Type Type);

/// <summary>
/// Makes a scan of the data of a table function.
/// </summary>
/// <param name="data">What the bind callback returned as the data.</param>
/// <param name="projected">The columns that the query reads, by their index in the function, in the order of the chunk.</param>
/// <param name="columnCount">How many columns the function has.</param>
delegate TableFunctionScan TableFunctionScanFactory(IEnumerable data, int[] projected, int columnCount);

class TableFunctionBindData(IReadOnlyList<ColumnInfo> columns, IEnumerable? data, Func<IReadOnlyList<ProjectedColumn>, IEnumerable>? dataFactory, ulong connectionId)
{
    public IReadOnlyList<ColumnInfo> Columns { get; } = columns;
    public IEnumerable? Data { get; } = data;
    public Func<IReadOnlyList<ProjectedColumn>, IEnumerable>? DataFactory { get; } = dataFactory;
    public ulong ConnectionId { get; } = connectionId;
}

class TableFunctionInitData(TableFunctionScan? scan, DuckDBLogicalType[] logicalTypes) : IDisposable
{
    public TableFunctionScan? Scan { get; } = scan;

    // The types of the columns that the query reads, made once for the scan and not for every chunk.
    public DuckDBLogicalType[] LogicalTypes { get; } = logicalTypes;

    // One writer for every column that the query reads. A scan is read by one thread, so the array is used again
    // for every chunk.
    public VectorDataWriterBase[] Writers { get; } = new VectorDataWriterBase[logicalTypes.Length];

    public void Dispose()
    {
        Scan?.Dispose();

        foreach (var logicalType in LogicalTypes)
        {
            logicalType.Dispose();
        }
    }
}
