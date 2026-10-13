using DuckDB.NET.Data.DataChunk.Writer;

namespace DuckDB.NET.Data.Connection;

class TableFunctionInfo(Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> bind, Func<IEnumerable, TableFunctionScan> createScan, string[] namedParameterNames)
{
    public Func<IReadOnlyList<IDuckDBValueReader>, IReadOnlyDictionary<string, IDuckDBValueReader>, TableFunction> Bind { get; } = bind;

    // Makes the scan of the data that Bind returned. It knows the type of the rows; nothing else here does.
    public Func<IEnumerable, TableFunctionScan> CreateScan { get; } = createScan;
    public string[] NamedParameterNames { get; } = namedParameterNames;
}

record NamedParameterDefinition(string Name, Type Type);

class TableFunctionBindData(IReadOnlyList<ColumnInfo> columns, IEnumerable? data, Func<IReadOnlyList<ProjectedColumn>, IEnumerable>? dataFactory, ulong connectionId)
{
    public IReadOnlyList<ColumnInfo> Columns { get; } = columns;
    public IEnumerable? Data { get; } = data;
    public Func<IReadOnlyList<ProjectedColumn>, IEnumerable>? DataFactory { get; } = dataFactory;
    public ulong ConnectionId { get; } = connectionId;
}

class TableFunctionInitData(int[] projected, TableFunctionScan? scan, DuckDBLogicalType[] logicalTypes, int columnCount) : IDisposable
{
    public int[] Projected { get; } = projected;
    public TableFunctionScan? Scan { get; } = scan;

    // The types of the projected columns, made once for the scan and not for every chunk.
    public DuckDBLogicalType[] LogicalTypes { get; } = logicalTypes;

    // One slot for every column of the function. A scan is read by one thread, so the array is used again for
    // every chunk.
    public IDuckDBDataWriter[] Writers { get; } = new IDuckDBDataWriter[columnCount];

    public void Dispose()
    {
        Scan?.Dispose();

        foreach (var logicalType in LogicalTypes)
        {
            logicalType.Dispose();
        }
    }
}
